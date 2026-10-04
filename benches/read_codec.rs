//! The read codec on synthetic 19-column batches shaped like a wide BigQuery table: every
//! scalar type, a STRUCT, an ARRAY<STRUCT> and two ARRAYs. Three arms differ only in the four
//! temporal fields: plain jiff types, the crate's wrappers, and integers. Each is timed one row
//! at a time, dropping each row, and collected into a `Vec` per batch.

use arrow_array::types::IntervalMonthDayNano;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    Float64Array, Int64Array, IntervalMonthDayNanoArray, ListArray, RecordBatch, StringArray,
    StructArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow_buffer::{i256, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields, Schema};
use bigquery::{
    BigQueryDate, BigQueryDateTime, BigQueryInterval, BigQueryResult, BigQueryTime,
    BigQueryTimestamp,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

const ROWS_PER_BATCH: usize = 10_000;
const BATCHES: usize = 10;

#[derive(Deserialize)]
#[allow(dead_code, reason = "the fields are decoded, never read")]
struct Addr {
    street: String,
    zip: Option<i64>,
}

#[derive(Deserialize)]
#[allow(dead_code, reason = "the fields are decoded, never read")]
struct Item {
    sku: String,
    qty: i64,
}

#[derive(Deserialize)]
#[allow(dead_code, reason = "the fields are decoded, never read")]
struct Wide<D, Ts, Dt, Tm> {
    id: i64,
    name: String,
    score: Option<f64>,
    active: bool,
    payload: serde_bytes::ByteBuf,
    day: D,
    at: Ts,
    local: Dt,
    tod: Tm,
    amount: String,
    big: String,
    doc: String,
    span: BigQueryInterval,
    geo: String,
    tags: Vec<String>,
    nums: Vec<i64>,
    addr: Option<Addr>,
    items: Vec<Item>,
    note: Option<String>,
}

type JiffRow = Wide<jiff::civil::Date, jiff::Timestamp, jiff::civil::DateTime, jiff::civil::Time>;
type WrapperRow = Wide<BigQueryDate, BigQueryTimestamp, BigQueryDateTime, BigQueryTime>;
type IntegerRow = Wide<i32, i64, i64, i64>;

fn ext(name: &str) -> HashMap<String, String> {
    HashMap::from([("ARROW:extension:name".to_string(), name.to_string())])
}

fn list_of(name: &str, per_row: usize, values: ArrayRef) -> (Field, ArrayRef) {
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(per_row, ROWS_PER_BATCH));
    let item = Arc::new(Field::new("item", values.data_type().clone(), true));
    let list = ListArray::new(item, offsets, values, None);
    (
        Field::new(name, list.data_type().clone(), false),
        Arc::new(list),
    )
}

fn wide_batch(seed: usize) -> RecordBatch {
    let n = ROWS_PER_BATCH;
    let i = |k: usize| (seed * n + k) as i64;
    let micros = |k: usize| 1_700_000_000_000_000 + i(k) * 1_000_003;
    let addr = StructArray::new(
        Fields::from(vec![
            Field::new("street", DataType::Utf8, true),
            Field::new("zip", DataType::Int64, true),
        ]),
        vec![
            Arc::new(StringArray::from_iter_values(
                (0..n).map(|k| format!("Storgatan {}", k % 97)),
            )) as ArrayRef,
            Arc::new(Int64Array::from_iter((0..n).map(|k| (k % 5 != 0).then_some(i(k))))),
        ],
        None,
    );
    let items = StructArray::new(
        Fields::from(vec![
            Field::new("sku", DataType::Utf8, true),
            Field::new("qty", DataType::Int64, true),
        ]),
        vec![
            Arc::new(StringArray::from_iter_values(
                (0..2 * n).map(|k| format!("sku-{k}")),
            )) as ArrayRef,
            Arc::new(Int64Array::from_iter_values((0..2 * n).map(|k| k as i64))),
        ],
        None,
    );
    let cols: Vec<(Field, ArrayRef)> = vec![
        (
            Field::new("id", DataType::Int64, false),
            Arc::new(Int64Array::from_iter_values((0..n).map(i))),
        ),
        (
            Field::new("name", DataType::Utf8, false),
            Arc::new(StringArray::from_iter_values(
                (0..n).map(|k| format!("Björn Åkesson {k}")),
            )),
        ),
        (
            Field::new("score", DataType::Float64, true),
            Arc::new(Float64Array::from_iter(
                (0..n).map(|k| (k % 3 != 0).then_some(k as f64 * 0.5)),
            )),
        ),
        (
            Field::new("active", DataType::Boolean, false),
            Arc::new(BooleanArray::from_iter((0..n).map(|k| Some(k % 2 == 0)))),
        ),
        (
            Field::new("payload", DataType::Binary, false),
            Arc::new(BinaryArray::from_iter_values(
                (0..n).map(|k| (k as u64).to_le_bytes()),
            )),
        ),
        (
            Field::new("day", DataType::Date32, false),
            Arc::new(Date32Array::from_iter_values((0..n).map(|k| 19_000 + (k % 3000) as i32))),
        ),
        {
            let a = TimestampMicrosecondArray::from_iter_values((0..n).map(micros))
                .with_timezone("UTC");
            (Field::new("at", a.data_type().clone(), false), Arc::new(a))
        },
        (
            Field::new(
                "local",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                false,
            )
            .with_metadata(ext("google:sqlType:datetime")),
            Arc::new(TimestampMicrosecondArray::from_iter_values((0..n).map(micros))),
        ),
        (
            Field::new(
                "tod",
                DataType::Time64(arrow_schema::TimeUnit::Microsecond),
                false,
            ),
            Arc::new(Time64MicrosecondArray::from_iter_values(
                (0..n).map(|k| (k as i64 * 7_919_000) % 86_400_000_000),
            )),
        ),
        {
            let a = Decimal128Array::from_iter_values((0..n).map(|k| k as i128 * 1_250_000_000))
                .with_precision_and_scale(38, 9)
                .expect("NUMERIC's Arrow type");
            (Field::new("amount", a.data_type().clone(), false), Arc::new(a))
        },
        {
            let a = Decimal256Array::from_iter_values(
                (0..n).map(|k| i256::from_i128(k as i128 * 10i128.pow(36))),
            )
            .with_precision_and_scale(76, 38)
            .expect("BIGNUMERIC's Arrow type");
            (Field::new("big", a.data_type().clone(), false), Arc::new(a))
        },
        (
            Field::new("doc", DataType::Utf8, false).with_metadata(ext("google:sqlType:json")),
            Arc::new(StringArray::from_iter_values(
                (0..n).map(|k| format!(r#"{{"k":{k},"tags":["a","b"]}}"#)),
            )),
        ),
        (
            Field::new(
                "span",
                DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
                false,
            )
            .with_metadata(ext("google:sqlType:interval")),
            Arc::new(IntervalMonthDayNanoArray::from_iter_values((0..n).map(|k| {
                IntervalMonthDayNano::new(k as i32 % 24, k as i32 % 30, k as i64 * 1_000_000)
            }))),
        ),
        (
            Field::new("geo", DataType::Utf8, false)
                .with_metadata(ext("google:sqlType:geography")),
            Arc::new(StringArray::from_iter_values(
                (0..n).map(|k| format!("POINT({} {})", k % 180, k % 90)),
            )),
        ),
        list_of(
            "tags",
            2,
            Arc::new(StringArray::from_iter_values(
                (0..2 * n).map(|k| format!("tag{}", k % 11)),
            )),
        ),
        list_of(
            "nums",
            3,
            Arc::new(Int64Array::from_iter_values((0..3 * n).map(|k| k as i64))),
        ),
        (
            Field::new("addr", addr.data_type().clone(), true),
            Arc::new(addr),
        ),
        list_of("items", 2, Arc::new(items)),
        (
            Field::new("note", DataType::Utf8, true),
            Arc::new(StringArray::from_iter(
                (0..n).map(|k| (k % 4 == 0).then(|| format!("anteckning {k}"))),
            )),
        ),
    ];
    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = cols.into_iter().unzip();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .expect("the synthetic columns form a batch")
}

/// Fails the benchmark before timing if any row of `batches` does not decode into `T`.
fn assert_decodes<T: DeserializeOwned>(batches: &[RecordBatch]) {
    for batch in batches {
        bigquery::__bench_decode_each(batch, |row: BigQueryResult<T>| {
            if let Err(err) = row {
                panic!("a synthetic row does not decode: {err}");
            }
        });
    }
}

fn arm<T: DeserializeOwned>(c: &mut Criterion, name: &str, batches: &[RecordBatch]) {
    assert_decodes::<T>(batches);
    let mut group = c.benchmark_group("read_codec");
    group.throughput(Throughput::Elements((ROWS_PER_BATCH * BATCHES) as u64));
    group.sample_size(20);
    group.bench_function(BenchmarkId::new("one row at a time", name), |b| {
        b.iter(|| {
            for batch in batches {
                bigquery::__bench_decode_each(batch, |row: BigQueryResult<T>| {
                    black_box(row.ok());
                });
            }
        })
    });
    group.bench_function(BenchmarkId::new("into Vec", name), |b| {
        b.iter(|| {
            for batch in batches {
                let mut rows = Vec::with_capacity(batch.num_rows());
                bigquery::__bench_decode_each(batch, |row: BigQueryResult<T>| rows.push(row));
                black_box(rows);
            }
        })
    });
    group.finish();
}

fn read_codec(c: &mut Criterion) {
    let batches: Vec<RecordBatch> = (0..BATCHES).map(wide_batch).collect();
    arm::<JiffRow>(c, "jiff fields", &batches);
    arm::<WrapperRow>(c, "wrappers", &batches);
    arm::<IntegerRow>(c, "integers", &batches);
}

criterion_group!(benches, read_codec);
criterion_main!(benches);
