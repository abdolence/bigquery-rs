//! Shared parts of the two Rust contenders: the scenario SQL, the generated rows, and the
//! line protocol the orchestrator (`python/orchestrate.py`) drives every client with.
//!
//! A client process starts, prints one `ready` line with its versions, then answers one
//! request line per measured run. Keeping the process alive between runs keeps its channels
//! and tokens warm, so the orchestrator can interleave the clients run by run without paying
//! a process start in every measurement.

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, Int64Builder, ListBuilder,
    StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, Write};
use std::sync::Arc;
use std::time::Instant;

/// Rows in the table scan and in every write run.
pub const SCAN_ROWS: i64 = 1_000_000;
/// Rows in the large-result query.
pub const LARGE_QUERY_ROWS: i64 = 200_000;
/// The scan table every reader reads in full.
pub const SCAN_TABLE: &str = "scan_1m";
/// Rows per Arrow record batch in the Arrow writes, so one batch goes as one `AppendRows`
/// request well under the official writer's 10 MB limit (about 5.5 MB measured).
pub const WRITE_BATCH_ROWS: usize = 25_000;

/// The scenarios, as named in the requests and the results.
#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Scenario {
    QueryConst,
    #[serde(rename = "query_1k")]
    Query1k,
    #[serde(rename = "query_200k_rows")]
    Query200kRows,
    #[serde(rename = "query_200k_arrow")]
    Query200kArrow,
    ScanRows,
    ScanArrow,
    Write,
    WriteArrow,
    Decode,
}

pub const SQL_CONST: &str = "SELECT 1 AS x";

pub fn sql_1k() -> String {
    "SELECT x, CONCAT('row_', CAST(x AS STRING)) AS s FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS x"
        .to_string()
}

pub fn sql_200k() -> String {
    format!(
        "SELECT x, CONCAT('row_', CAST(x AS STRING)) AS s, x * 1.5 AS f, MOD(x, 2) = 0 AS b \
         FROM UNNEST(GENERATE_ARRAY(1, {LARGE_QUERY_ROWS})) AS x"
    )
}

/// The select list that generates row `x` of the scan table. The write scenario generates the
/// same values in Rust with [`PlainRow::generate`], so both tables have the same shape.
pub fn scan_select() -> String {
    format!(
        "SELECT \
           x AS id, x * 7 AS i1, MOD(x * 31, 1000003) AS i2, -x AS i3, \
           x / 4 AS f1, x * 0.5 AS f2, IF(MOD(x, 10) = 0, NULL, x * 0.25) AS f3, \
           CONCAT('name_', CAST(x AS STRING)) AS s1, TO_HEX(MD5(CAST(x AS STRING))) AS s2, \
           IF(MOD(x, 7) = 0, NULL, REPEAT('z', MOD(x, 20))) AS s3, \
           MOD(x, 2) = 0 AS b1, IF(MOD(x, 5) = 0, NULL, MOD(x, 3) = 0) AS b2, \
           CAST(x AS NUMERIC) / 100 AS n1, CAST(MOD(x, 99999) AS NUMERIC) + NUMERIC '0.123456789' AS n2, \
           DATE_ADD(DATE '2000-01-01', INTERVAL MOD(x, 9000) DAY) AS d1, \
           DATE_ADD(DATE '1970-01-01', INTERVAL MOD(x * 13, 20000) DAY) AS d2, \
           TIMESTAMP_ADD(TIMESTAMP '2020-01-01 00:00:00+00', INTERVAL x SECOND) AS t1, \
           TIMESTAMP_MICROS(1600000000000000 + x * 1234567) AS t2, \
           STRUCT(x AS a, CONCAT('s', CAST(MOD(x, 100) AS STRING)) AS b) AS st, \
           GENERATE_ARRAY(x, x + MOD(x, 5)) AS arr \
         FROM UNNEST(GENERATE_ARRAY(1, {SCAN_ROWS})) AS x"
    )
}

/// The scan columns, in table order; every reader reads all of them.
pub const SCAN_COLUMNS: [&str; 20] = [
    "id", "i1", "i2", "i3", "f1", "f2", "f3", "s1", "s2", "s3", "b1", "b2", "n1", "n2", "d1", "d2",
    "t1", "t2", "st", "arr",
];

/// One scan row in plain types, the input both writers start from.
#[derive(Clone, Debug)]
pub struct PlainRow {
    pub id: i64,
    pub i1: i64,
    pub i2: i64,
    pub i3: i64,
    pub f1: f64,
    pub f2: f64,
    pub f3: Option<f64>,
    pub s1: String,
    pub s2: String,
    pub s3: Option<String>,
    pub b1: bool,
    pub b2: Option<bool>,
    pub n1: rust_decimal::Decimal,
    pub n2: rust_decimal::Decimal,
    pub d1: jiff::civil::Date,
    pub d2: jiff::civil::Date,
    pub t1: jiff::Timestamp,
    pub t2: jiff::Timestamp,
    pub st_a: i64,
    pub st_b: String,
    pub arr: Vec<i64>,
}

impl PlainRow {
    /// Row `x` with the values [`scan_select`] gives it, except `s2`, which is a fixed-width
    /// hex string of the same length rather than an MD5.
    pub fn generate(x: i64) -> Self {
        let epoch2000 = jiff::civil::date(2000, 1, 1);
        let epoch1970 = jiff::civil::date(1970, 1, 1);
        let t2020 = jiff::Timestamp::from_second(1_577_836_800).expect("in range");
        Self {
            id: x,
            i1: x * 7,
            i2: (x * 31) % 1_000_003,
            i3: -x,
            f1: x as f64 / 4.0,
            f2: x as f64 * 0.5,
            f3: (x % 10 != 0).then_some(x as f64 * 0.25),
            s1: format!("name_{x}"),
            s2: format!(
                "{:032x}",
                (x as u128).wrapping_mul(0x9E37_79B9_7F4A_7C15_F39C_C060_5CED_C834)
            ),
            s3: (x % 7 != 0).then(|| "z".repeat((x % 20) as usize)),
            b1: x % 2 == 0,
            b2: (x % 5 != 0).then_some(x % 3 == 0),
            n1: rust_decimal::Decimal::new(x, 2),
            n2: rust_decimal::Decimal::new((x % 99_999) * 1_000_000_000 + 123_456_789, 9),
            d1: epoch2000
                .checked_add(jiff::Span::new().days(x % 9000))
                .expect("in range"),
            d2: epoch1970
                .checked_add(jiff::Span::new().days((x * 13) % 20_000))
                .expect("in range"),
            t1: t2020
                .checked_add(jiff::SignedDuration::from_secs(x))
                .expect("in range"),
            t2: jiff::Timestamp::from_microsecond(1_600_000_000_000_000 + x * 1_234_567)
                .expect("in range"),
            st_a: x,
            st_b: format!("s{}", x % 100),
            arr: (x..=x + x % 5).collect(),
        }
    }
}

/// The rows of one write run.
pub fn write_rows() -> Vec<PlainRow> {
    (1..=SCAN_ROWS).map(PlainRow::generate).collect()
}

impl PlainRow {
    /// The scan table's columns as BigQuery's Storage Write expects them in Arrow: NUMERIC as
    /// `Decimal128(38, 9)`, DATE as `Date32` and TIMESTAMP as microseconds in UTC.
    pub fn arrow_schema() -> Schema {
        let st = Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let dec = DataType::Decimal128(38, 9);
        Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("i1", DataType::Int64, true),
            Field::new("i2", DataType::Int64, true),
            Field::new("i3", DataType::Int64, true),
            Field::new("f1", DataType::Float64, true),
            Field::new("f2", DataType::Float64, true),
            Field::new("f3", DataType::Float64, true),
            Field::new("s1", DataType::Utf8, true),
            Field::new("s2", DataType::Utf8, true),
            Field::new("s3", DataType::Utf8, true),
            Field::new("b1", DataType::Boolean, true),
            Field::new("b2", DataType::Boolean, true),
            Field::new("n1", dec.clone(), true),
            Field::new("n2", dec, true),
            Field::new("d1", DataType::Date32, true),
            Field::new("d2", DataType::Date32, true),
            Field::new("t1", ts.clone(), true),
            Field::new("t2", ts, true),
            Field::new("st", DataType::Struct(st), true),
            Field::new(
                "arr",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
        ])
    }

    /// Builds one Arrow batch of `schema` (see [`PlainRow::arrow_schema`]) from plain rows, the step
    /// a caller of an Arrow writer does themselves.
    pub fn record_batch(schema: &Arc<Schema>, rows: &[PlainRow]) -> anyhow::Result<RecordBatch> {
        let n = rows.len();
        let ints = |f: fn(&PlainRow) -> i64| -> ArrayRef {
            let mut b = Int64Builder::with_capacity(n);
            rows.iter().for_each(|r| b.append_value(f(r)));
            Arc::new(b.finish())
        };
        let floats = |f: fn(&PlainRow) -> Option<f64>| -> ArrayRef {
            let mut b = Float64Builder::with_capacity(n);
            rows.iter().for_each(|r| b.append_option(f(r)));
            Arc::new(b.finish())
        };
        let strings = |f: fn(&PlainRow) -> Option<&str>| -> ArrayRef {
            let mut b = StringBuilder::with_capacity(n, n * 16);
            rows.iter().for_each(|r| b.append_option(f(r)));
            Arc::new(b.finish())
        };
        let bools = |f: fn(&PlainRow) -> Option<bool>| -> ArrayRef {
            let mut b = BooleanBuilder::with_capacity(n);
            rows.iter().for_each(|r| b.append_option(f(r)));
            Arc::new(b.finish())
        };
        let decimals = |f: fn(&PlainRow) -> rust_decimal::Decimal| -> anyhow::Result<ArrayRef> {
            let mut b = Decimal128Builder::with_capacity(n);
            rows.iter().for_each(|r| b.append_value(decimal9(f(r))));
            Ok(Arc::new(b.finish().with_precision_and_scale(38, 9)?))
        };
        let dates = |f: fn(&PlainRow) -> jiff::civil::Date| -> ArrayRef {
            let mut b = Date32Builder::with_capacity(n);
            rows.iter().for_each(|r| b.append_value(days(f(r))));
            Arc::new(b.finish())
        };
        let stamps = |f: fn(&PlainRow) -> jiff::Timestamp| -> ArrayRef {
            let mut b = TimestampMicrosecondBuilder::with_capacity(n);
            rows.iter()
                .for_each(|r| b.append_value(f(r).as_microsecond()));
            Arc::new(b.finish().with_timezone("UTC"))
        };
        let st_fields = match schema.field_with_name("st")?.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => anyhow::bail!("st is a STRUCT"),
        };
        let st = StructArray::try_new(
            st_fields,
            vec![ints(|r| r.st_a), strings(|r| Some(r.st_b.as_str()))],
            None,
        )?;
        let mut arr = ListBuilder::with_capacity(Int64Builder::with_capacity(n * 3), n)
            .with_field(Arc::new(Field::new("item", DataType::Int64, true)));
        for r in rows {
            arr.values().append_slice(&r.arr);
            arr.append(true);
        }
        let columns: Vec<ArrayRef> = vec![
            ints(|r| r.id),
            ints(|r| r.i1),
            ints(|r| r.i2),
            ints(|r| r.i3),
            floats(|r| Some(r.f1)),
            floats(|r| Some(r.f2)),
            floats(|r| r.f3),
            strings(|r| Some(r.s1.as_str())),
            strings(|r| Some(r.s2.as_str())),
            strings(|r| r.s3.as_deref()),
            bools(|r| Some(r.b1)),
            bools(|r| r.b2),
            decimals(|r| r.n1)?,
            decimals(|r| r.n2)?,
            dates(|r| r.d1),
            dates(|r| r.d2),
            stamps(|r| r.t1),
            stamps(|r| r.t2),
            Arc::new(st),
            Arc::new(arr.finish()),
        ];
        Ok(RecordBatch::try_new(schema.clone(), columns)?)
    }
}

/// The NUMERIC value as the unscaled `i128` of a scale-9 decimal.
fn decimal9(d: rust_decimal::Decimal) -> i128 {
    let mut d = d;
    d.rescale(9);
    d.mantissa()
}

fn days(d: jiff::civil::Date) -> i32 {
    (d - jiff::civil::date(1970, 1, 1)).get_days()
}

/// The rows of one Arrow write run, as batches of [`WRITE_BATCH_ROWS`] rows.
pub fn write_batches() -> anyhow::Result<Vec<RecordBatch>> {
    let schema = Arc::new(PlainRow::arrow_schema());
    write_rows()
        .chunks(WRITE_BATCH_ROWS)
        .map(|chunk| PlainRow::record_batch(&schema, chunk))
        .collect()
}

/// A request from the orchestrator.
#[derive(Deserialize, Debug)]
pub struct Request {
    pub scenario: Scenario,
    /// The orchestrator's run number; 0 is the warm-up.
    pub run: u32,
}

/// What one run measured. `secs` is the wall-clock time of the run inside this process.
#[derive(Serialize, Debug, Default)]
pub struct Outcome {
    pub secs: f64,
    pub rows: u64,
    /// The bytes the client sent (writes) or decoded from the wire (Arrow reads), when it knows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    /// Where the rows came from: `inline`, `storage_read`, `rest_pages`, etc.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub streams: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_billed: Option<i64>,
    /// Anything else a client measured, e.g. the time a REST client spent in its typed decode.
    #[serde(skip_serializing_if = "serde_json::Map::is_empty")]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// The command line every Rust contender takes: `--project P --dataset D --run-label L --location LOC`.
#[derive(Debug, Clone)]
pub struct Args {
    pub project: String,
    pub dataset: String,
    pub run_label: String,
    /// The dataset's location, passed explicitly on every query so that generated rows are
    /// computed in the same region the tables live in.
    pub location: String,
    pub rest: Vec<String>,
}

impl Args {
    pub fn parse() -> anyhow::Result<Self> {
        let mut project = None;
        let mut dataset = None;
        let mut run_label = None;
        let mut location = None;
        let mut rest = Vec::new();
        let mut it = std::env::args().skip(1);
        while let Some(arg) = it.next() {
            match arg.as_str() {
                "--project" => project = it.next(),
                "--dataset" => dataset = it.next(),
                "--run-label" => run_label = it.next(),
                "--location" => location = it.next(),
                _ => rest.push(arg),
            }
        }
        Ok(Self {
            project: project.ok_or_else(|| anyhow::anyhow!("--project is required"))?,
            dataset: dataset.ok_or_else(|| anyhow::anyhow!("--dataset is required"))?,
            run_label: run_label.unwrap_or_else(|| "manual".to_string()),
            location: location.unwrap_or_else(|| "europe-north2".to_string()),
            rest,
        })
    }
}

/// Prints the `ready` line, then answers requests until stdin closes. A failed run, or a
/// request for a scenario this build does not know, is reported as an `error` line and the
/// loop goes on, so one scenario a client cannot do never takes the others down with it.
pub async fn serve<F>(info: serde_json::Value, mut run: F) -> anyhow::Result<()>
where
    F: AsyncFnMut(&Request) -> anyhow::Result<Outcome>,
{
    let stdout = std::io::stdout();
    writeln!(
        stdout.lock(),
        "{}",
        serde_json::json!({ "ready": true, "info": info })
    )?;
    let stdin = std::io::stdin();
    let mut line = String::new();
    loop {
        line.clear();
        if stdin.lock().read_line(&mut line)? == 0 {
            return Ok(());
        }
        let reply = match serde_json::from_str::<Request>(line.trim()) {
            Ok(request) => match run(&request).await {
                Ok(outcome) => serde_json::to_value(&outcome)?,
                Err(err) => serde_json::json!({ "error": format!("{err:#}") }),
            },
            Err(err) => serde_json::json!({ "error": format!("n/a: {err}") }),
        };
        writeln!(stdout.lock(), "{reply}")?;
        stdout.lock().flush()?;
    }
}

/// Times `f` with a monotonic clock, in seconds.
pub async fn timed<T>(f: impl AsyncFnOnce() -> anyhow::Result<T>) -> anyhow::Result<(f64, T)> {
    let start = Instant::now();
    let value = f().await?;
    Ok((start.elapsed().as_secs_f64(), value))
}

/// The machine's available parallelism, recorded because this crate's default Storage Read
/// stream count follows it and the official client's read arm is given the same count.
pub fn parallelism() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1)
}
