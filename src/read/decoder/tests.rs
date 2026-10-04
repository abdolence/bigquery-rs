use super::*;
use crate::errors::{BigQueryError, BigQuerySerializationError};
use crate::types::civil;
use crate::types::testkit::{canonical, canonical_range, Canonical};
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

fn batch(cols: Vec<(Field, ArrayRef)>) -> RecordBatch {
    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = cols.into_iter().unzip();
    RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), arrays)
        .expect("the test columns form a batch")
}

fn col<A: Array + 'static>(name: &str, a: A) -> (Field, ArrayRef) {
    (Field::new(name, a.data_type().clone(), true), Arc::new(a))
}

fn required<A: Array + 'static>(name: &str, a: A) -> (Field, ArrayRef) {
    (Field::new(name, a.data_type().clone(), false), Arc::new(a))
}

fn ext_meta(name: &str) -> HashMap<String, String> {
    HashMap::from([("ARROW:extension:name".to_string(), name.to_string())])
}

fn range_meta() -> HashMap<String, String> {
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
    let a = ListArray::new(item, OffsetBuffer::new(offsets.into()), values, None);
    (
        Field::new(name, a.data_type().clone(), false).with_metadata(metadata),
        Arc::new(a),
    )
}

fn list(name: &str, offsets: Vec<i32>, values: ArrayRef) -> (Field, ArrayRef) {
    list_with(name, offsets, values, HashMap::new())
}

fn strukt(
    name: &str,
    children: Vec<(Field, ArrayRef)>,
    nulls: Option<Vec<bool>>,
) -> (Field, ArrayRef) {
    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = children.into_iter().unzip();
    let a = StructArray::new(Fields::from(fields), arrays, nulls.map(NullBuffer::from));
    (Field::new(name, a.data_type().clone(), true), Arc::new(a))
}

fn ts_micros(s: &str) -> i64 {
    civil::parse_timestamp(s).expect("a valid test timestamp")
}

fn rows<T: DeserializeOwned>(b: &RecordBatch) -> Vec<T> {
    decode_rows::<T>(b, 0)
        .into_iter()
        .collect::<BigQueryResult<Vec<T>>>()
        .unwrap_or_else(|e| panic!("every row decodes: {e}"))
}

fn row<T: DeserializeOwned>(b: &RecordBatch, i: usize) -> Result<T, BigQuerySerializationError> {
    match decode_rows::<T>(b, 0).swap_remove(i) {
        Ok(v) => Ok(v),
        Err(BigQueryError::DeserializeError(details)) => Err(details),
        Err(other) => panic!("expected a deserialize error, got {other:?}"),
    }
}

fn row_err<T: DeserializeOwned>(b: &RecordBatch, i: usize) -> BigQuerySerializationError {
    match row::<T>(b, i) {
        Ok(_) => panic!("row {i} decoded, and it must fail"),
        Err(e) => e,
    }
}

fn all_scalars() -> RecordBatch {
    batch(vec![
        col("i", Int64Array::from(vec![i64::MIN])),
        col("f", Float64Array::from(vec![f64::NEG_INFINITY])),
        col("b", BooleanArray::from(vec![true])),
        col("s", StringArray::from(vec!["héllo 世界 🦀"])),
        col("y", BinaryArray::from_vec(vec![&[0u8, 255, 16][..]])),
        col("d", Date32Array::from(vec![-719162])),
        col("t", Time64MicrosecondArray::from(vec![86_399_999_999])),
        {
            let (f, a) = col(
                "dt",
                TimestampMicrosecondArray::from(vec![ts_micros("2024-02-29T12:34:56.789012Z")]),
            );
            (f.with_metadata(ext_meta("google:sqlType:datetime")), a)
        },
        col(
            "ts",
            TimestampMicrosecondArray::from(vec![ts_micros("1969-07-20T20:17:40Z")])
                .with_timezone("UTC"),
        ),
        col(
            "num",
            Decimal128Array::from(vec![10i128.pow(38) - 1])
                .with_precision_and_scale(38, 9)
                .expect("NUMERIC's Arrow type"),
        ),
        col(
            "big",
            Decimal256Array::from(vec![i256::MIN])
                .with_precision_and_scale(76, 38)
                .expect("BIGNUMERIC's Arrow type"),
        ),
        {
            let (f, a) = col(
                "js",
                StringArray::from(vec![r#"{"a":1,"b":[true,null]}"#]),
            );
            (f.with_metadata(ext_meta("google:sqlType:json")), a)
        },
        {
            let (f, a) = col(
                "iv",
                IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNano::new(
                    14,
                    -3,
                    14_706_000_789_000,
                )]),
            );
            (f.with_metadata(ext_meta("google:sqlType:interval")), a)
        },
    ])
}

#[derive(Deserialize, Debug, PartialEq)]
struct Scalars {
    i: i64,
    f: f64,
    b: bool,
    s: String,
    y: Vec<u8>,
    d: jiff::civil::Date,
    t: jiff::civil::Time,
    dt: jiff::civil::DateTime,
    ts: jiff::Timestamp,
    num: String,
    big: String,
    js: BigQueryJson<serde_json::Value>,
    iv: crate::BigQueryInterval,
}

#[test]
fn scalars_decode_into_their_rust_forms() {
    let got: Vec<Scalars> = rows(&all_scalars());
    assert_eq!(
        got,
        vec![Scalars {
            i: i64::MIN,
            f: f64::NEG_INFINITY,
            b: true,
            s: "héllo 世界 🦀".into(),
            y: vec![0, 255, 16],
            d: jiff::civil::date(1, 1, 1),
            t: "23:59:59.999999".parse().expect("valid"),
            dt: "2024-02-29T12:34:56.789012".parse().expect("valid"),
            ts: "1969-07-20T20:17:40Z".parse().expect("valid"),
            num: "99999999999999999999999999999.999999999".into(),
            big: "-578960446186580977117854925043439539266.34992332820282019728792003956564819968"
                .into(),
            js: BigQueryJson(serde_json::json!({"a": 1, "b": [true, null]})),
            iv: crate::BigQueryInterval {
                months: 14,
                days: -3,
                nanos: 14_706_000_789_000,
            },
        }]
    );
}

#[test]
fn null_cells_need_option_and_errors_carry_row_and_path() {
    let b = batch(vec![col("x", Int64Array::from(vec![Some(1), None]))]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct O {
        x: Option<i64>,
    }
    #[derive(Deserialize, Debug)]
    struct R {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        x: i64,
    }
    assert_eq!(rows::<O>(&b), vec![O { x: Some(1) }, O { x: None }]);
    let decoded = decode_rows::<R>(&b, 40);
    assert!(decoded[0].is_ok());
    let Err(BigQueryError::DeserializeError(e)) = &decoded[1] else {
        panic!("the NULL row must fail, got {:?}", decoded[1]);
    };
    assert_eq!(e.kind, BigQueryCodecErrorKind::NullForNonOption);
    assert_eq!(e.row, Some(41));
    assert_eq!(e.path, "x");
}

fn nested() -> RecordBatch {
    let inner = strukt(
        "inner",
        vec![col("x", Date32Array::from(vec![Some(19782), None, None]))],
        Some(vec![true, false, true]),
    );
    let tags = list(
        "tags",
        vec![0, 2, 2, 3],
        Arc::new(StringArray::from(vec!["x", "y", "z"])),
    );
    let rec = strukt(
        "rec",
        vec![
            col("a", Int64Array::from(vec![Some(7), None, None])),
            tags,
            inner,
        ],
        Some(vec![true, false, true]),
    );
    let kv = StructArray::new(
        Fields::from(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("v", DataType::Float64, true),
        ]),
        vec![
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
            Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0), None])),
        ],
        None,
    );
    let recs = list("recs", vec![0, 2, 2, 3], Arc::new(kv));
    batch(vec![col("id", Int64Array::from(vec![0, 1, 2])), rec, recs])
}

#[derive(Deserialize, Debug, PartialEq)]
struct Inner {
    x: Option<jiff::civil::Date>,
}

#[derive(Deserialize, Debug, PartialEq)]
struct Rec {
    a: Option<i64>,
    tags: Vec<String>,
    inner: Option<Inner>,
}

#[derive(Deserialize, Debug, PartialEq)]
struct Kv {
    k: Option<String>,
    v: Option<f64>,
}

#[derive(Deserialize, Debug, PartialEq)]
struct NestedRow {
    id: i64,
    rec: Option<Rec>,
    recs: Vec<Kv>,
}

#[test]
fn nested_struct_list_and_list_of_struct() {
    assert_eq!(
        rows::<NestedRow>(&nested()),
        vec![
            NestedRow {
                id: 0,
                rec: Some(Rec {
                    a: Some(7),
                    tags: vec!["x".into(), "y".into()],
                    inner: Some(Inner {
                        x: Some(jiff::civil::date(2024, 2, 29))
                    }),
                }),
                recs: vec![
                    Kv {
                        k: Some("a".into()),
                        v: Some(1.0)
                    },
                    Kv {
                        k: Some("b".into()),
                        v: Some(2.0)
                    }
                ],
            },
            NestedRow {
                id: 1,
                rec: None,
                recs: vec![]
            },
            NestedRow {
                id: 2,
                rec: Some(Rec {
                    a: None,
                    tags: vec!["z".into()],
                    inner: Some(Inner { x: None }),
                }),
                recs: vec![Kv { k: None, v: None }],
            },
        ]
    );
}

#[test]
fn null_struct_into_bare_struct_is_an_error() {
    #[derive(Deserialize, Debug)]
    struct Bare {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        rec: Rec,
    }
    let e = row_err::<Bare>(&nested(), 1);
    assert_eq!(
        (e.row, e.path.as_str(), e.kind),
        (Some(1), "rec", BigQueryCodecErrorKind::NullForNonOption)
    );
}

#[test]
fn error_path_names_the_nested_list_element() {
    #[derive(Deserialize, Debug)]
    struct KvBad {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        v: Option<String>,
    }
    #[derive(Deserialize, Debug)]
    struct Bad {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        recs: Vec<KvBad>,
    }
    let e = row_err::<Bad>(&nested(), 0);
    assert_eq!(
        (e.row, e.path.as_str(), e.kind),
        (Some(0), "recs[0].v", BigQueryCodecErrorKind::TypeMismatch)
    );

    #[derive(Deserialize, Debug)]
    struct RecBadTag {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        tags: Vec<i64>,
    }
    #[derive(Deserialize, Debug)]
    struct BadTag {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        rec: Option<RecBadTag>,
    }
    let e = row_err::<BadTag>(&nested(), 2);
    assert_eq!((e.row, e.path.as_str()), (Some(2), "rec.tags[0]"));
}

#[test]
fn unnamed_columns_are_never_visited() {
    let b = batch(vec![
        col("id", Int64Array::from(vec![1, 2])),
        col("odd", Float32Array::from(vec![1.0, 2.0])),
    ]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct Id {
        id: i64,
    }
    assert_eq!(rows::<Id>(&b), vec![Id { id: 1 }, Id { id: 2 }]);
}

#[test]
fn unknown_arrow_type_fails_only_rows_that_name_it() {
    let rec = strukt(
        "rec",
        vec![col("odd", Float32Array::from(vec![1.0, 2.0]))],
        Some(vec![false, true]),
    );
    let b = batch(vec![col("id", Int64Array::from(vec![1, 2])), rec]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct Odd {
        odd: f32,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct R {
        id: i64,
        rec: Option<Odd>,
    }
    assert_eq!(row::<R>(&b, 0), Ok(R { id: 1, rec: None }));
    let e = row_err::<R>(&b, 1);
    assert_eq!(e.kind, BigQueryCodecErrorKind::UnsupportedType);
    assert_eq!(e.path, "rec.odd");
    assert!(e.message.contains("Float32"), "{}", e.message);
}

#[test]
fn the_field_plan_is_built_once_per_batch() {
    let b = nested();
    let decoder = BatchDecoder::new(&b);
    for i in 0..3 {
        decoder
            .row::<NestedRow>(i)
            .unwrap_or_else(|e| panic!("row {i}: {e}"));
    }
    // One plan for the row, one each for rec, rec.inner and the recs element.
    assert_eq!(decoder.plans_built(), 4);
}

#[test]
fn bignumeric_numeric_and_interval_alternate_targets() {
    #[derive(Deserialize, Debug, PartialEq)]
    struct Alt {
        num: f64,
        big: f64,
        iv: String,
        num_dec: BigQueryDecimal<String>,
    }
    let b = all_scalars();
    let mut cols: Vec<(Field, ArrayRef)> = b
        .schema()
        .fields()
        .iter()
        .zip(b.columns())
        .map(|(f, a)| (f.as_ref().clone(), a.clone()))
        .collect();
    cols.push(col(
        "num_dec",
        Decimal128Array::from(vec![1_500_000_000i128])
            .with_precision_and_scale(38, 9)
            .expect("NUMERIC's Arrow type"),
    ));
    let got: Alt = rows(&batch(cols)).remove(0);
    assert_eq!(got.num, 1e29);
    assert_eq!(
        got.big,
        "-578960446186580977117854925043439539266.34992332820282019728792003956564819968"
            .parse::<f64>()
            .expect("valid")
    );
    assert_eq!(got.iv, "1-2 -3 4:5:6.000789");
    assert_eq!(got.num_dec, BigQueryDecimal("1.5".to_string()));
}

#[test]
fn timestamp_forms_cover_the_full_bigquery_range() {
    let max = civil::TIMESTAMP_MAX_MICROS;
    let b = batch(vec![col(
        "ts",
        TimestampMicrosecondArray::from(vec![max, -1]).with_timezone("UTC"),
    )]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct S {
        ts: String,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct I {
        ts: i64,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct J {
        ts: jiff::Timestamp,
    }
    assert_eq!(
        row::<S>(&b, 0).map(|r| r.ts),
        Ok("9999-12-31T23:59:59.999999Z".to_string())
    );
    assert_eq!(row::<I>(&b, 0).map(|r| r.ts), Ok(max));
    assert_eq!(
        row::<J>(&b, 1).map(|r| r.ts),
        Ok("1969-12-31T23:59:59.999999Z".parse().expect("valid"))
    );
}

#[test]
fn timestamp_above_jiff_max_fails_only_that_row() {
    let b = batch(vec![col(
        "ts",
        TimestampMicrosecondArray::from(vec![civil::TIMESTAMP_MAX_MICROS, 0]).with_timezone("UTC"),
    )]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct J {
        ts: jiff::Timestamp,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct W {
        ts: BigQueryTimestamp,
    }
    let e = row_err::<J>(&b, 0);
    assert_eq!(
        (e.row, e.path.as_str(), e.kind),
        (Some(0), "ts", BigQueryCodecErrorKind::OutOfRange)
    );
    assert_eq!(row::<J>(&b, 1), Ok(J { ts: jiff::Timestamp::UNIX_EPOCH }));
    assert_eq!(row_err::<W>(&b, 0).kind, BigQueryCodecErrorKind::OutOfRange);
    assert_eq!(
        row::<W>(&b, 1),
        Ok(W {
            ts: BigQueryTimestamp(jiff::Timestamp::UNIX_EPOCH)
        })
    );
}

#[test]
fn borrowed_str_and_bytes_live_as_long_as_the_batch() {
    let b = batch(vec![
        col("s", StringArray::from(vec!["abc"])),
        col("y", BinaryArray::from_vec(vec![&b"\x01\x02"[..]])),
    ]);
    #[derive(Deserialize)]
    struct B<'a> {
        s: &'a str,
        y: &'a [u8],
    }
    let decoder = BatchDecoder::new(&b);
    let r: B = decoder.row(0).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!((r.s, r.y), ("abc", &[1u8, 2][..]));
}

#[test]
fn dynamic_rows_through_deserialize_any() {
    let b = nested();
    assert_eq!(
        row::<serde_json::Value>(&b, 1),
        Ok(serde_json::json!({"id": 1, "rec": null, "recs": []}))
    );
    let v = row::<serde_json::Value>(&b, 0).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(v["rec"]["inner"]["x"], "2024-02-29");
    assert_eq!(v["recs"][1]["v"], 2.0);
}

#[test]
fn temporal_wrappers_decode_from_integers() {
    let day = 19_782; // 2024-02-29
    let at = ts_micros("2024-02-29T12:34:56.789012Z");
    let (local_field, local) = col("local", TimestampMicrosecondArray::from(vec![at]));
    let b = batch(vec![
        col("day", Date32Array::from(vec![day])),
        col("tod", Time64MicrosecondArray::from(vec![45_296_000_001])),
        (
            local_field.with_metadata(ext_meta("google:sqlType:datetime")),
            local,
        ),
        col(
            "at",
            TimestampMicrosecondArray::from(vec![at]).with_timezone("UTC"),
        ),
        col("no_day", Date32Array::from(vec![None::<i32>])),
        list(
            "ats",
            vec![0, 2],
            Arc::new(TimestampMicrosecondArray::from(vec![0, at]).with_timezone("UTC")),
        ),
        col("day_with", Date32Array::from(vec![day])),
        col("at_opt", TimestampMicrosecondArray::from(vec![at]).with_timezone("UTC")),
    ]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct W {
        day: BigQueryDate,
        tod: BigQueryTime,
        local: BigQueryDateTime,
        at: BigQueryTimestamp,
        no_day: Option<BigQueryDate>,
        ats: Vec<BigQueryTimestamp>,
        #[serde(with = "crate::serialize_as_date")]
        day_with: jiff::civil::Date,
        #[serde(with = "crate::serialize_as_optional_timestamp")]
        at_opt: Option<jiff::Timestamp>,
    }
    let ts: jiff::Timestamp = "2024-02-29T12:34:56.789012Z".parse().expect("valid");
    assert_eq!(
        rows::<W>(&b),
        vec![W {
            day: BigQueryDate(jiff::civil::date(2024, 2, 29)),
            tod: BigQueryTime(jiff::civil::time(12, 34, 56, 1_000)),
            local: BigQueryDateTime("2024-02-29T12:34:56.789012".parse().expect("valid")),
            at: BigQueryTimestamp(ts),
            no_day: None,
            ats: vec![
                BigQueryTimestamp(jiff::Timestamp::UNIX_EPOCH),
                BigQueryTimestamp(ts)
            ],
            day_with: jiff::civil::date(2024, 2, 29),
            at_opt: Some(ts),
        }]
    );
}

#[test]
fn temporal_wrapper_on_another_temporal_column_is_type_mismatch() {
    let (dt_field, dt) = col("local", TimestampMicrosecondArray::from(vec![0]));
    let b = batch(vec![
        col("at", TimestampMicrosecondArray::from(vec![0]).with_timezone("UTC")),
        (dt_field.with_metadata(ext_meta("google:sqlType:datetime")), dt),
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
    let e = row_err::<DateOnTimestamp>(&b, 0);
    assert_eq!(
        (e.path.as_str(), e.kind),
        ("at", BigQueryCodecErrorKind::TypeMismatch)
    );
    let e = row_err::<TimestampOnDateTime>(&b, 0);
    assert_eq!(
        (e.path.as_str(), e.kind),
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
    let b = batch(vec![col("num", numeric), col("big", big)]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct N {
        num: i64,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct B {
        big: i128,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct Narrow {
        num: u8,
    }
    assert_eq!(row::<N>(&b, 0), Ok(N { num: 5 }));
    let e = row_err::<N>(&b, 1);
    assert_eq!(
        (e.path.as_str(), e.kind),
        ("num", BigQueryCodecErrorKind::OutOfRange)
    );
    assert_eq!(row::<N>(&b, 2), Ok(N { num: -2 }));
    assert_eq!(row::<B>(&b, 0), Ok(B { big: i128::MAX }));
    assert_eq!(row::<B>(&b, 1), Ok(B { big: 1 }));
    assert_eq!(row_err::<Narrow>(&b, 2).kind, BigQueryCodecErrorKind::OutOfRange);
}

/// A characterisation of BigQuery's input: a REQUIRED RANGE column has no validity buffer for
/// its children, so an unbounded end arrives as the epoch value and cannot be told apart.
#[test]
fn required_range_unbounded_end_reads_as_epoch() {
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
    let b = batch(vec![
        (
            Field::new("r", required.data_type().clone(), false).with_metadata(range_meta()),
            Arc::new(required) as ArrayRef,
        ),
        (
            Field::new("n", nullable.data_type().clone(), true).with_metadata(range_meta()),
            Arc::new(nullable),
        ),
    ]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct R {
        r: BigQueryRange<jiff::civil::Date>,
        n: Option<BigQueryRange<jiff::civil::Date>>,
    }
    let epoch = jiff::civil::date(1970, 1, 1);
    assert_eq!(
        row::<R>(&b, 0),
        Ok(R {
            r: BigQueryRange {
                start: Some(epoch),
                end: Some(jiff::civil::date(1970, 1, 6))
            },
            n: Some(BigQueryRange {
                start: None,
                end: Some(jiff::civil::date(1970, 1, 6))
            }),
        })
    );
}

#[test]
fn json_column_parses_into_any_shape_but_a_string() {
    let meta = || ext_meta("google:sqlType:json");
    let (f, a) = col(
        "js",
        StringArray::from(vec![Some(r#"{"a":1,"b":["x"]}"#), Some("null"), None]),
    );
    let (lf, la) = list_with(
        "ajs",
        vec![0, 2, 2, 2],
        Arc::new(StringArray::from(vec![r#"{"a":2,"b":[]}"#, "[1,2]"])),
        meta(),
    );
    let b = batch(vec![(f.with_metadata(meta()), a), (lf, la)]);

    #[derive(Deserialize, Debug, PartialEq)]
    struct Doc {
        a: i64,
        b: Vec<String>,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct Typed {
        js: Option<Doc>,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct Values {
        js: Option<serde_json::Value>,
        ajs: Vec<serde_json::Value>,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct Raw {
        js: Option<String>,
        ajs: Vec<String>,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct Maps {
        js: HashMap<String, serde_json::Value>,
    }

    assert_eq!(
        row::<Typed>(&b, 0),
        Ok(Typed { js: Some(Doc { a: 1, b: vec!["x".into()] }) })
    );
    assert_eq!(
        row::<Values>(&b, 0),
        Ok(Values {
            js: Some(serde_json::json!({"a": 1, "b": ["x"]})),
            ajs: vec![serde_json::json!({"a": 2, "b": []}), serde_json::json!([1, 2])],
        })
    );
    assert_eq!(
        row::<Values>(&b, 1),
        Ok(Values { js: Some(serde_json::Value::Null), ajs: vec![] }),
        "JSON null stays apart from SQL NULL"
    );
    assert_eq!(row::<Values>(&b, 2), Ok(Values { js: None, ajs: vec![] }));
    assert_eq!(
        row::<Raw>(&b, 0),
        Ok(Raw {
            js: Some(r#"{"a":1,"b":["x"]}"#.into()),
            ajs: vec![r#"{"a":2,"b":[]}"#.into(), "[1,2]".into()],
        }),
        "a String gets the JSON text as it is"
    );
    assert_eq!(
        row::<Maps>(&b, 0).map(|m| m.js.len()),
        Ok(2)
    );
    let e = row_err::<Typed>(&b, 1);
    assert_eq!(
        (e.path.as_str(), e.kind),
        ("js", BigQueryCodecErrorKind::Custom),
        "JSON null is not an Option's None"
    );
}

#[test]
fn bytes_column_into_json_value_is_an_error() {
    let b = batch(vec![col("y", BinaryArray::from_vec(vec![&b"\x01"[..]]))]);
    #[derive(Deserialize, Debug)]
    struct J {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        y: serde_json::Value,
    }
    let e = row_err::<J>(&b, 0);
    assert_eq!(
        (e.path.as_str(), e.kind),
        ("y", BigQueryCodecErrorKind::TypeMismatch)
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

fn s(x: &str) -> String {
    x.to_string()
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
            Rename { id: 1, name: s("ann") },
            Rename { id: 2, name: s("bob") }
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
            RenameAll { id: 1, camel_field: 10 },
            RenameAll { id: 2, camel_field: 20 }
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
        [Alias { id: 1, name: s("ann") }, Alias { id: 2, name: s("bob") }]
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
            Skip { id: 1, cache: None, extra: s("e1") },
            Skip { id: 2, cache: None, extra: s("e2") }
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
            WithDefault { id: 1, missing: 0, tags: vec![], extra: s("e1") },
            WithDefault { id: 2, missing: 0, tags: vec![], extra: s("e2") }
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
            OptionMissing { id: 1, not_a_column: None },
            OptionMissing { id: 2, not_a_column: None }
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
    let e = row_err::<RequiredMissing>(&full_batch(), 0);
    assert_eq!(e.kind, BigQueryCodecErrorKind::Custom);
    assert!(e.message.contains("missing field `not_a_column`"), "{}", e.message);
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
                rest: FlatRest { user_name: s("ann"), extra: s("e1") }
            },
            Flat {
                id: 2,
                rest: FlatRest { user_name: s("bob"), extra: s("e2") }
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
    let b = full_batch();
    for i in 0..2 {
        let e = row_err::<Deny>(&b, i);
        assert!(e.message.contains("unknown field"), "row {i}: {}", e.message);
    }
}

#[test]
fn deny_unknown_fields_accepts_exact_columns() {
    let b = batch(vec![required("id", Int64Array::from(vec![1, 2]))]);
    assert_eq!(rows::<Deny>(&b), [Deny { id: 1 }, Deny { id: 2 }]);
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
            Reordered { extra: s("e1"), id: 1 },
            Reordered { extra: s("e2"), id: 2 }
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
    let b = alias_batch();
    for i in 0..2 {
        let e = row_err::<Alias>(&b, i);
        assert!(e.message.contains("duplicate field"), "row {i}: {}", e.message);
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
    let e = row_err::<Alias>(&alias_batch(), 0);
    assert!(e.message.contains("duplicate field"), "{}", e.message);
}

#[test]
fn alias_on_the_first_field_with_every_name_a_column_is_an_error() {
    let b = batch(vec![
        required("a", Int64Array::from(vec![1])),
        required("x", Int64Array::from(vec![2])),
        required("b", Int64Array::from(vec![3])),
    ]);
    #[derive(Deserialize, Debug)]
    struct First {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        #[serde(alias = "x")]
        a: i64,
        #[allow(dead_code, reason = "decoded only to see it fail")]
        b: i64,
    }
    let e = row_err::<First>(&b, 0);
    assert!(e.message.contains("duplicate field"), "{}", e.message);
}

#[test]
fn alias_with_only_the_alias_column_decodes() {
    let b = batch(vec![
        required("id", Int64Array::from(vec![1])),
        required("user_name", StringArray::from(vec!["ann"])),
    ]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct Alias {
        id: i64,
        #[serde(alias = "user_name")]
        name: String,
    }
    assert_eq!(rows::<Alias>(&b), [Alias { id: 1, name: s("ann") }]);
}

#[test]
fn alias_and_default_decode_by_name() {
    #[derive(Deserialize, Debug, PartialEq)]
    struct AliasDefault {
        #[serde(alias = "user_name", default)]
        a: String,
        extra: String,
        #[serde(default)]
        other: String,
    }
    assert_eq!(
        rows::<AliasDefault>(&full_batch()),
        [
            AliasDefault { a: s("ann"), extra: s("e1"), other: s("") },
            AliasDefault { a: s("bob"), extra: s("e2"), other: s("") }
        ]
    );
}

#[test]
fn plain_struct_keeps_index_keys() {
    let b = all_scalars();
    let decoder = BatchDecoder::new(&b);
    decoder
        .row::<Scalars>(0)
        .unwrap_or_else(|e| panic!("{e}"));
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
    let b = batch(vec![
        required("id", Int64Array::from(vec![1])),
        required("extra", StringArray::from(vec!["e1"])),
    ]);
    let decoder = BatchDecoder::new(&b);
    assert_eq!(
        decoder.row::<TrailingSkip>(0).map_err(|e| e.to_string()),
        Ok(TrailingSkip { id: 1, extra: s("e1"), cache: None })
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
        required("rest", StringArray::from(vec!["r"])),
    ])
}

#[test]
fn ignored_any_field_falls_back_to_string_keys() {
    let b = tail_batch();
    let decoder = BatchDecoder::new(&b);
    assert_eq!(
        decoder.row::<Tail>(0).map_err(|e| e.to_string()),
        Ok(Tail { id: 7, rest: IgnoredAny })
    );
    assert_eq!(decoder.name_key_plans(), 1);
}

#[test]
fn swallowed_probe_error_still_redoes_the_row() {
    fn lenient<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Tail>, D::Error> {
        Ok(Option::<Tail>::deserialize(d).unwrap_or(None))
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct Outer {
        #[serde(deserialize_with = "lenient")]
        rec: Option<Tail>,
    }
    let tail = tail_batch();
    let rec = StructArray::from(tail);
    let b = batch(vec![col("rec", rec)]);
    assert_eq!(
        row::<Outer>(&b, 0),
        Ok(Outer {
            rec: Some(Tail { id: 7, rest: IgnoredAny })
        })
    );
}

// The round trip of every type and mode: canonical values built into the Arrow arrays
// BigQuery sends, decoded into each Rust target of the type, and mapped back.

fn opt_ints<T: Copy>(values: &[Option<Canonical>], f: impl Fn(&Canonical) -> T) -> Vec<Option<T>> {
    values.iter().map(|v| v.as_ref().map(&f)).collect()
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
                .map(|v| v.map(|d| i32::try_from(d).expect("DATE days fit i32")))
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
fn arrow_values(kind: BqKind, values: &[Option<Canonical>]) -> (ArrayRef, HashMap<String, String>) {
    let none = HashMap::new();
    let unexpected = |v: &Canonical| -> ! { panic!("{kind:?} column got {v:?}") };
    match kind {
        BqKind::Int64 => (
            Arc::new(Int64Array::from(opt_ints(values, |v| match v {
                Canonical::Int64(x) => *x,
                v => unexpected(v),
            }))),
            none,
        ),
        BqKind::Float64 => (
            Arc::new(Float64Array::from(opt_ints(values, |v| match v {
                Canonical::Float64(bits) => f64::from_bits(*bits),
                v => unexpected(v),
            }))),
            none,
        ),
        BqKind::Bool => (
            Arc::new(BooleanArray::from(opt_ints(values, |v| match v {
                Canonical::Bool(x) => *x,
                v => unexpected(v),
            }))),
            none,
        ),
        BqKind::String | BqKind::Geography | BqKind::Json => {
            let text: Vec<Option<String>> = values
                .iter()
                .map(|v| {
                    v.as_ref().map(|v| match v {
                        Canonical::String(s) | Canonical::Geography(s) => s.clone(),
                        Canonical::Json(j) => j.to_string(),
                        v => unexpected(v),
                    })
                })
                .collect();
            let meta = match kind {
                BqKind::Json => ext_meta("google:sqlType:json"),
                BqKind::Geography => {
                    let mut m = ext_meta("google:sqlType:geography");
                    m.insert(
                        "ARROW:extension:metadata".into(),
                        r#"{"encoding": "WKT"}"#.into(),
                    );
                    m
                }
                _ => none,
            };
            (Arc::new(StringArray::from(text)), meta)
        }
        BqKind::Bytes => {
            let bytes: Vec<Option<&[u8]>> = values
                .iter()
                .map(|v| {
                    v.as_ref().map(|v| match v {
                        Canonical::Bytes(b) => b.as_slice(),
                        v => unexpected(v),
                    })
                })
                .collect();
            (Arc::new(BinaryArray::from_opt_vec(bytes)), none)
        }
        BqKind::Date => (
            Arc::new(Date32Array::from(opt_ints(values, |v| match v {
                Canonical::Date(d) => *d,
                v => unexpected(v),
            }))),
            none,
        ),
        BqKind::Time => (
            Arc::new(Time64MicrosecondArray::from(opt_ints(values, |v| match v {
                Canonical::Time(t) => *t,
                v => unexpected(v),
            }))),
            none,
        ),
        BqKind::DateTime => (
            Arc::new(TimestampMicrosecondArray::from(opt_ints(values, |v| match v {
                Canonical::DateTime(t) => *t,
                v => unexpected(v),
            }))),
            ext_meta("google:sqlType:datetime"),
        ),
        BqKind::Timestamp => (
            Arc::new(
                TimestampMicrosecondArray::from(opt_ints(values, |v| match v {
                    Canonical::Timestamp(t) => *t,
                    v => unexpected(v),
                }))
                .with_timezone("UTC"),
            ),
            none,
        ),
        BqKind::Numeric => (
            Arc::new(
                Decimal128Array::from(opt_ints(values, |v| match v {
                    Canonical::Numeric(x) => *x,
                    v => unexpected(v),
                }))
                .with_precision_and_scale(38, 9)
                .expect("NUMERIC's Arrow type"),
            ),
            none,
        ),
        BqKind::BigNumeric => (
            Arc::new(
                Decimal256Array::from(opt_ints(values, |v| match v {
                    Canonical::BigNumeric(x) => *x,
                    v => unexpected(v),
                }))
                .with_precision_and_scale(76, 38)
                .expect("BIGNUMERIC's Arrow type"),
            ),
            none,
        ),
        BqKind::Interval => (
            Arc::new(IntervalMonthDayNanoArray::from(opt_ints(values, |v| {
                match v {
                    Canonical::Interval(iv) => IntervalMonthDayNano::new(iv.months, iv.days, iv.nanos),
                    v => unexpected(v),
                }
            }))),
            ext_meta("google:sqlType:interval"),
        ),
        BqKind::Range => {
            let element = values
                .iter()
                .flatten()
                .find_map(|v| match v {
                    Canonical::Range { element, .. } => Some(*element),
                    _ => None,
                })
                .unwrap_or(BigQueryRangeElementType::Date);
            let bound = |pick: fn(&Canonical) -> Option<i64>| -> Vec<Option<i64>> {
                values.iter().map(|v| v.as_ref().and_then(pick)).collect()
            };
            let start = bound(|v| match v {
                Canonical::Range { start, .. } => *start,
                _ => None,
            });
            let end = bound(|v| match v {
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
                range_meta(),
            )
        }
        BqKind::Struct => panic!("STRUCT columns are covered by the nested tests"),
    }
}

/// A batch of `n` (NULLABLE, every third row NULL), `r` (REQUIRED) and `a` (REPEATED, up to
/// three values from the row on) over `values`.
fn mode_batch(kind: BqKind, values: &[Canonical]) -> RecordBatch {
    let nullable: Vec<Option<Canonical>> = values
        .iter()
        .enumerate()
        .map(|(i, v)| (i % 3 != 2).then(|| v.clone()))
        .collect();
    let all: Vec<Option<Canonical>> = values.iter().cloned().map(Some).collect();
    let mut offsets = vec![0i32];
    let mut flat = Vec::new();
    for i in 0..values.len() {
        flat.extend(values[i..(i + 3).min(values.len())].iter().cloned().map(Some));
        offsets.push(i32::try_from(flat.len()).expect("a small test list"));
    }
    let (n, n_meta) = arrow_values(kind, &nullable);
    let (r, r_meta) = arrow_values(kind, &all);
    let (a, a_meta) = arrow_values(kind, &flat);
    batch(vec![
        (
            Field::new("n", n.data_type().clone(), true).with_metadata(n_meta),
            n,
        ),
        (
            Field::new("r", r.data_type().clone(), false).with_metadata(r_meta),
            r,
        ),
        list_with("a", offsets, a, a_meta),
    ])
}

#[derive(Deserialize)]
struct ModeRow<X> {
    n: Option<X>,
    r: X,
    a: Vec<X>,
}

/// Decodes `values` in every mode into `X` and checks that `back` gives each value again.
fn check_target<X: DeserializeOwned>(
    kind: BqKind,
    target: &str,
    values: &[Canonical],
    back: impl Fn(X) -> Canonical,
) -> Result<(), TestCaseError> {
    if values.is_empty() {
        return Ok(());
    }
    let b = mode_batch(kind, values);
    for (i, decoded) in decode_rows::<ModeRow<X>>(&b, 0).into_iter().enumerate() {
        let decoded = decoded.map_err(|e| {
            TestCaseError::fail(format!("{kind:?} into {target}, row {i}: {e}"))
        })?;
        let expected_n = (i % 3 != 2).then(|| values[i].clone());
        prop_assert_eq!(decoded.n.map(&back), expected_n, "{:?} n into {}", kind, target);
        prop_assert_eq!(back(decoded.r), values[i].clone(), "{:?} r into {}", kind, target);
        let a: Vec<Canonical> = decoded.a.into_iter().map(&back).collect();
        prop_assert_eq!(
            a,
            values[i..(i + 3).min(values.len())].to_vec(),
            "{:?} a into {}",
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
        .filter(|v| match v {
            Canonical::Timestamp(t) => *t <= max,
            Canonical::Range { start, end, .. } => {
                start.is_none_or(|s| s <= max) && end.is_none_or(|e| e <= max)
            }
            _ => true,
        })
        .cloned()
        .collect()
}

fn checks(kind: BqKind, v: &[Canonical]) -> Result<(), TestCaseError> {
    use Canonical as C;
    let ok = |r: Result<i64, CodecError>| r.unwrap_or_else(|e| panic!("{e}"));
    match kind {
        BqKind::Int64 => {
            check_target(kind, "i64", v, C::Int64)?;
            check_target(kind, "i128", v, |x: i128| {
                C::Int64(i64::try_from(x).expect("an INT64 value"))
            })?;
        }
        BqKind::Float64 => check_target(kind, "f64", v, |x: f64| C::Float64(x.to_bits()))?,
        BqKind::Bool => check_target(kind, "bool", v, C::Bool)?,
        BqKind::String => {
            check_target(kind, "String", v, C::String)?;
            check_target(kind, "Box<str>", v, |x: Box<str>| C::String(x.into()))?;
        }
        BqKind::Bytes => {
            check_target(kind, "Vec<u8>", v, C::Bytes)?;
            check_target(kind, "ByteBuf", v, |x: serde_bytes::ByteBuf| {
                C::Bytes(x.into_vec())
            })?;
        }
        BqKind::Date => {
            check_target(kind, "jiff Date", v, |d: jiff::civil::Date| {
                C::Date(civil::date_days(d).expect("in range"))
            })?;
            check_target(kind, "BigQueryDate", v, |d: BigQueryDate| {
                C::Date(civil::date_days(d.0).expect("in range"))
            })?;
            check_target(kind, "String", v, |s: String| {
                C::Date(civil::parse_date(&s).expect("DATE text"))
            })?;
            check_target(kind, "i32", v, C::Date)?;
        }
        BqKind::Time => {
            check_target(kind, "jiff Time", v, |t: jiff::civil::Time| {
                C::Time(civil::time_micros(t))
            })?;
            check_target(kind, "BigQueryTime", v, |t: BigQueryTime| {
                C::Time(civil::time_micros(t.0))
            })?;
            check_target(kind, "String", v, |s: String| {
                C::Time(ok(civil::parse_time(&s)))
            })?;
            check_target(kind, "i64", v, C::Time)?;
        }
        BqKind::DateTime => {
            check_target(kind, "jiff DateTime", v, |t: jiff::civil::DateTime| {
                C::DateTime(ok(civil::datetime_micros(t)))
            })?;
            check_target(kind, "BigQueryDateTime", v, |t: BigQueryDateTime| {
                C::DateTime(ok(civil::datetime_micros(t.0)))
            })?;
            check_target(kind, "String", v, |s: String| {
                C::DateTime(ok(civil::parse_datetime(&s)))
            })?;
            check_target(kind, "i64", v, C::DateTime)?;
        }
        BqKind::Timestamp => {
            let safe = below_jiff_max(v);
            check_target(kind, "jiff Timestamp", &safe, |t: jiff::Timestamp| {
                C::Timestamp(ok(civil::timestamp_micros(t)))
            })?;
            check_target(kind, "BigQueryTimestamp", &safe, |t: BigQueryTimestamp| {
                C::Timestamp(ok(civil::timestamp_micros(t.0)))
            })?;
            check_target(kind, "String", v, |s: String| {
                C::Timestamp(ok(civil::parse_timestamp(&s)))
            })?;
            check_target(kind, "i64", v, C::Timestamp)?;
        }
        BqKind::Numeric => {
            let numeric = |s: &str| {
                C::Numeric(
                    decimal::parse_numeric(s)
                        .expect("NUMERIC text")
                        .to_i128()
                        .expect("NUMERIC fits i128"),
                )
            };
            check_target(kind, "String", v, |s: String| numeric(&s))?;
            check_target(kind, "BigQueryDecimal<String>", v, |s: BigQueryDecimal<String>| {
                numeric(&s.0)
            })?;
        }
        BqKind::BigNumeric => {
            let big = |s: &str| C::BigNumeric(decimal::parse_bignumeric(s).expect("text"));
            check_target(kind, "String", v, |s: String| big(&s))?;
            check_target(kind, "BigQueryDecimal<String>", v, |s: BigQueryDecimal<String>| {
                big(&s.0)
            })?;
        }
        BqKind::Geography => check_target(kind, "String", v, C::Geography)?,
        BqKind::Json => {
            check_target(kind, "String", v, |s: String| {
                C::Json(serde_json::from_str(&s).expect("JSON text"))
            })?;
            check_target(kind, "BigQueryJson<Value>", v, |j: BigQueryJson<serde_json::Value>| {
                C::Json(j.0)
            })?;
            check_target(kind, "Value", v, C::Json)?;
        }
        BqKind::Interval => {
            check_target(kind, "BigQueryInterval", v, C::Interval)?;
            check_target(kind, "String", v, |s: String| {
                C::Interval(
                    crate::BigQueryInterval::parse_bq(&s).expect("INTERVAL text"),
                )
            })?;
        }
        BqKind::Range => range_checks(v)?,
        BqKind::Struct => {}
    }
    Ok(())
}

fn range_checks(v: &[Canonical]) -> Result<(), TestCaseError> {
    let Some(Canonical::Range { element, .. }) = v.first() else {
        return Ok(());
    };
    let element = *element;
    let kind = BqKind::Range;
    let back = move |start: Option<i64>, end: Option<i64>| Canonical::Range { element, start, end };
    let ok = |r: Result<i64, CodecError>| r.unwrap_or_else(|e| panic!("{e}"));
    match element {
        BigQueryRangeElementType::Date => {
            let days = |d: jiff::civil::Date| i64::from(civil::date_days(d).expect("in range"));
            check_target(kind, "BigQueryRange<Date>", v, |r: BigQueryRange<jiff::civil::Date>| {
                back(r.start.map(days), r.end.map(days))
            })?;
            check_target(kind, "BigQueryRange<BigQueryDate>", v, |r: BigQueryRange<BigQueryDate>| {
                back(r.start.map(|d| days(d.0)), r.end.map(|d| days(d.0)))
            })?;
            check_target(kind, "BigQueryRange<i32>", v, |r: BigQueryRange<i32>| {
                back(r.start.map(i64::from), r.end.map(i64::from))
            })?;
        }
        BigQueryRangeElementType::DateTime => {
            let micros = |d: jiff::civil::DateTime| ok(civil::datetime_micros(d));
            check_target(
                kind,
                "BigQueryRange<DateTime>",
                v,
                |r: BigQueryRange<jiff::civil::DateTime>| {
                    back(r.start.map(micros), r.end.map(micros))
                },
            )?;
            check_target(kind, "BigQueryRange<i64>", v, |r: BigQueryRange<i64>| {
                back(r.start, r.end)
            })?;
        }
        BigQueryRangeElementType::Timestamp => {
            let micros = |t: BigQueryTimestamp| ok(civil::timestamp_micros(t.0));
            check_target(
                kind,
                "BigQueryRange<BigQueryTimestamp>",
                &below_jiff_max(v),
                |r: BigQueryRange<BigQueryTimestamp>| back(r.start.map(micros), r.end.map(micros)),
            )?;
            check_target(kind, "BigQueryRange<String>", v, |r: BigQueryRange<String>| {
                let parse = |s: String| ok(civil::parse_timestamp(&s));
                back(r.start.map(parse), r.end.map(parse))
            })?;
            check_target(kind, "BigQueryRange<i64>", v, |r: BigQueryRange<i64>| {
                back(r.start, r.end)
            })?;
        }
    }
    Ok(())
}

#[test]
fn every_type_and_mode_decodes_its_canonical_arrow_form() {
    let kinds = [
        BqKind::Int64,
        BqKind::Float64,
        BqKind::Bool,
        BqKind::String,
        BqKind::Bytes,
        BqKind::Date,
        BqKind::Time,
        BqKind::DateTime,
        BqKind::Timestamp,
        BqKind::Numeric,
        BqKind::BigNumeric,
        BqKind::Geography,
        BqKind::Json,
        BqKind::Interval,
    ];
    let config = proptest::test_runner::Config::with_cases(64);
    for kind in kinds {
        let mut runner = proptest::test_runner::TestRunner::new(config.clone());
        let strategy = proptest::collection::vec(canonical(kind), 1..8);
        runner
            .run(&strategy, |values| checks(kind, &values))
            .unwrap_or_else(|e| panic!("{kind:?}: {e}"));
    }
    for element in [
        BigQueryRangeElementType::Date,
        BigQueryRangeElementType::DateTime,
        BigQueryRangeElementType::Timestamp,
    ] {
        let mut runner = proptest::test_runner::TestRunner::new(config.clone());
        let strategy = proptest::collection::vec(canonical_range(element), 1..8);
        runner
            .run(&strategy, |values| checks(BqKind::Range, &values))
            .unwrap_or_else(|e| panic!("RANGE<{element:?}>: {e}"));
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
    let b = batch(vec![col("p", numeric), col("big", big), tags]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct P {
        p: String,
        big: String,
        tags: Vec<String>,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct Whole {
        p: i64,
        big: i64,
    }
    assert_eq!(
        rows::<P>(&b),
        [
            P { p: s("12345678.91"), big: s("-0.015"), tags: vec![s("0.7")] },
            P { p: s("5"), big: s("2"), tags: vec![] }
        ]
    );
    assert_eq!(row::<Whole>(&b, 1), Ok(Whole { p: 5, big: 2 }));
    assert_eq!(row_err::<Whole>(&b, 0).kind, BigQueryCodecErrorKind::OutOfRange);
}
