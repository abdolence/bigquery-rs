//! The write codec on synthetic 19-column rows shaped like the prototype's `t_wide`, with the
//! four temporal columns in three forms: plain jiff fields, the crate's wrappers, and the
//! BigQuery integers. One reused buffer, as the prototype's "reused buffer" arm.

use bigquery::*;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use serde::Serialize;
use serde_bytes::ByteBuf;
use std::hint::black_box;

const ROWS: usize = 100_000;

#[derive(Serialize, Clone)]
struct Addr {
    city: String,
    zip: Option<i64>,
    loc: Option<String>,
}

#[derive(Serialize, Clone)]
struct Item {
    sku: String,
    qty: i64,
    price: f64,
}

/// One wide row with its temporal columns as `D`, `Ts`, `Dt` and `T`.
#[derive(Serialize, Clone)]
struct Wide<D, Ts, Dt, T> {
    id: i64,
    name: String,
    score: Option<f64>,
    active: bool,
    payload: ByteBuf,
    day: D,
    at: Ts,
    local: Dt,
    tod: T,
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

fn field(
    name: &str,
    field_type: BigQueryFieldType,
    mode: BigQueryFieldMode,
) -> BigQueryFieldSchema {
    BigQueryFieldSchema {
        name: name.into(),
        field_type,
        mode,
        description: None,
        default_value_expression: None,
    }
}

fn wide_schema() -> BigQueryTableSchema {
    use BigQueryFieldMode::{Nullable, Repeated, Required};
    use BigQueryFieldType as FieldType;
    BigQueryTableSchema {
        fields: vec![
            field("id", FieldType::Int64, Required),
            field("name", FieldType::String { max_length: None }, Required),
            field("score", FieldType::Float64, Nullable),
            field("active", FieldType::Bool, Required),
            field("payload", FieldType::Bytes { max_length: None }, Required),
            field("day", FieldType::Date, Required),
            field("at", FieldType::Timestamp, Required),
            field("local", FieldType::DateTime, Required),
            field("tod", FieldType::Time, Required),
            field("amount", FieldType::Numeric(None), Required),
            field("big", FieldType::BigNumeric(None), Required),
            field("doc", FieldType::Json, Required),
            field("span", FieldType::Interval, Required),
            field("geo", FieldType::Geography, Required),
            field("tags", FieldType::String { max_length: None }, Repeated),
            field("nums", FieldType::Int64, Repeated),
            field(
                "addr",
                FieldType::Struct(vec![
                    field("city", FieldType::String { max_length: None }, Required),
                    field("zip", FieldType::Int64, Nullable),
                    field("loc", FieldType::Geography, Nullable),
                ]),
                Nullable,
            ),
            field(
                "items",
                FieldType::Struct(vec![
                    field("sku", FieldType::String { max_length: None }, Required),
                    field("qty", FieldType::Int64, Required),
                    field("price", FieldType::Float64, Required),
                ]),
                Repeated,
            ),
            field("note", FieldType::String { max_length: None }, Nullable),
        ],
    }
}

/// Row `i` of the deterministic data set, in its jiff form.
fn jiff_row(i: i64) -> JiffRow {
    let base: jiff::Timestamp = "2026-10-04T12:34:56.123456Z".parse().expect("valid");
    let at = base
        .checked_add(jiff::SignedDuration::from_micros(i * 1_000_003))
        .expect("in range");
    let day = jiff::civil::date(2026, 1, 1)
        .checked_add(jiff::Span::new().days(i % 3650))
        .expect("in range");
    let tod = jiff::civil::Time::new(
        (i % 24) as i8,
        (i % 60) as i8,
        (i * 7 % 60) as i8,
        (i % 1000) as i32 * 1000,
    )
    .expect("valid");
    Wide {
        id: i,
        name: format!("name-{i}"),
        score: (i % 10 != 0).then_some(i as f64 * 0.25),
        active: i % 2 == 0,
        payload: ByteBuf::from((i as u64).to_le_bytes().to_vec()),
        day,
        at,
        local: day.to_datetime(tod),
        tod,
        amount: format!("{}.{:02}", i / 100, i % 100),
        big: format!("{i}.000000000000000000000000000001"),
        doc: format!(r#"{{"i":{i},"k":"v{}"}}"#, i % 7),
        span: BigQueryInterval {
            months: (i % 25) as i32,
            days: (i % 31) as i32,
            nanos: (i % 86_400) * 1_000_000_000,
        },
        geo: format!("POINT({} {})", i % 180, i % 90),
        tags: (0..(i % 4)).map(|k| format!("t{k}")).collect(),
        nums: (0..(i % 5)).map(|k| i * 10 + k).collect(),
        addr: (i % 5 != 0).then(|| Addr {
            city: format!("city-{}", i % 100),
            zip: (i % 3 != 0).then_some(10_000 + i % 90_000),
            loc: None,
        }),
        items: (0..(i % 3))
            .map(|k| Item {
                sku: format!("sku-{k}"),
                qty: k + 1,
                price: k as f64 * 1.5,
            })
            .collect(),
        note: (i % 4 == 0).then(|| "note".to_string()),
    }
}

fn with_temporals<D, Ts, Dt, T>(
    row: &JiffRow,
    day: D,
    at: Ts,
    local: Dt,
    tod: T,
) -> Wide<D, Ts, Dt, T> {
    let row = row.clone();
    Wide {
        id: row.id,
        name: row.name,
        score: row.score,
        active: row.active,
        payload: row.payload,
        day,
        at,
        local,
        tod,
        amount: row.amount,
        big: row.big,
        doc: row.doc,
        span: row.span,
        geo: row.geo,
        tags: row.tags,
        nums: row.nums,
        addr: row.addr,
        items: row.items,
        note: row.note,
    }
}

fn wrapper_row(row: &JiffRow) -> WrapperRow {
    with_temporals(
        row,
        BigQueryDate(row.day),
        BigQueryTimestamp(row.at),
        BigQueryDateTime(row.local),
        BigQueryTime(row.tod),
    )
}

const MICROS_PER_DAY: i64 = 86_400_000_000;

fn time_micros(t: jiff::civil::Time) -> i64 {
    ((i64::from(t.hour()) * 60 + i64::from(t.minute())) * 60 + i64::from(t.second())) * 1_000_000
        + i64::from(t.subsec_nanosecond() / 1000)
}

fn integer_row(row: &JiffRow) -> IntegerRow {
    let days = row
        .day
        .since(jiff::civil::date(1970, 1, 1))
        .expect("in range")
        .get_days();
    with_temporals(
        row,
        days,
        row.at.as_microsecond(),
        i64::from(days) * MICROS_PER_DAY + time_micros(row.tod),
        time_micros(row.tod),
    )
}

fn arm<R: Serialize>(c: &mut Criterion, name: &str, rows: &[R]) {
    let mut encoder = BigQueryWriteCodecBench::new(&wide_schema());
    let mut buf = Vec::with_capacity(256 * 1024 * 1024);
    for row in rows.iter().take(10) {
        encoder.encode(row, &mut buf).expect("the row encodes");
    }
    let mut group = c.benchmark_group("write codec, wide rows");
    group.sample_size(10);
    group.throughput(Throughput::Elements(rows.len() as u64));
    group.bench_function(name, |b| {
        b.iter(|| {
            buf.clear();
            for row in rows {
                encoder.encode(row, &mut buf).expect("the row encodes");
            }
            black_box(buf.len());
        })
    });
    group.finish();
}

fn write_codec(c: &mut Criterion) {
    let jiff: Vec<JiffRow> = (0..ROWS as i64).map(jiff_row).collect();
    let wrappers: Vec<WrapperRow> = jiff.iter().map(wrapper_row).collect();
    let integers: Vec<IntegerRow> = jiff.iter().map(integer_row).collect();
    arm(c, "jiff fields", &jiff);
    arm(c, "wrappers", &wrappers);
    arm(c, "integers", &integers);
}

criterion_group!(benches, write_codec);
criterion_main!(benches);
