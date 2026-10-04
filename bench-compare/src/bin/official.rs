//! Google's official Rust client, `google-cloud-bigquery`, as a contender.
//!
//! Queries go through its `BigQuery` client, whose rows come back as JSON pages of
//! `jobs.getQueryResults` over REST and convert to structs with `#[derive(FromRow)]`. Table
//! scans go through its raw Storage Read client, which hands out Arrow IPC bytes; decoding
//! them is left to the caller, so the harness does it with `arrow-ipc` as a user would. Writes
//! go through its Arrow writer on the default stream.
//!
//! `official --project P --dataset D --run-label L`

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, Int64Builder, ListBuilder,
    StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch, StructArray};
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use bench_compare::*;
use futures::StreamExt;
use google_cloud_bigquery::client::{BigQuery, Read, Write};
use google_cloud_bigquery::model::arrow_serialization_options::CompressionCodec;
use google_cloud_bigquery::model::read_session::TableReadOptions;
use google_cloud_bigquery::model::{
    ArrowRecordBatch, ArrowSchema, ArrowSerializationOptions, DataFormat, ReadSession,
};
use google_cloud_bigquery::query::{FromRow, FromSql, Row};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Append requests in flight at once. The official writer leaves the window to the caller;
/// this is the same window as the `bigquery` crate's default, so the two writers pipeline
/// alike.
const WRITE_WINDOW: usize = 8;
/// Rows per Arrow append request, sized so that one request stays well under the 10 MB
/// `AppendRows` limit for these rows (about 5.5 MB measured).
const WRITE_BATCH_ROWS: usize = 25_000;

#[derive(FromRow)]
#[allow(dead_code, reason = "decoded to be timed, never read")]
struct SmallRow {
    x: i64,
}

#[derive(FromRow)]
#[allow(dead_code, reason = "decoded to be timed, never read")]
struct Row1k {
    x: i64,
    s: String,
}

#[derive(FromRow)]
#[allow(dead_code, reason = "decoded to be timed, never read")]
struct Row200k {
    x: i64,
    s: String,
    f: f64,
    b: bool,
}

#[derive(FromSql)]
#[allow(dead_code, reason = "decoded to be timed, never read")]
struct Inner {
    a: i64,
    b: String,
}

#[derive(FromRow)]
#[allow(dead_code, reason = "decoded to be timed, never read")]
struct ScanRow {
    id: i64,
    i1: i64,
    i2: i64,
    i3: i64,
    f1: f64,
    f2: f64,
    f3: Option<f64>,
    s1: String,
    s2: String,
    s3: Option<String>,
    b1: bool,
    b2: Option<bool>,
    n1: rust_decimal::Decimal,
    n2: rust_decimal::Decimal,
    d1: google_cloud_type::model::Date,
    d2: google_cloud_type::model::Date,
    t1: google_cloud_wkt::Timestamp,
    t2: google_cloud_wkt::Timestamp,
    st: Inner,
    arr: Vec<i64>,
}

struct Official {
    bq: BigQuery,
    read: Read,
    write: Write,
    args: Args,
    write_rows: Option<Vec<PlainRow>>,
}

impl Official {
    /// Runs `sql` and reads every row as `T`, timing the `Row` to `T` conversion apart from
    /// the whole, since the conversion is the only part of the typed path that is not I/O.
    async fn typed_query<T>(&self, sql: String) -> anyhow::Result<Outcome>
    where
        T: TryFrom<Row>,
        <T as TryFrom<Row>>::Error: std::error::Error + Send + Sync + 'static,
    {
        let (secs, (rows, billed, convert, job_created)) = timed(async || {
            let done = self
                .bq
                .query(sql)
                .set_use_query_cache(false)
                .set_location(self.args.location.clone())
                .set_labels([
                    ("bench_client", "official"),
                    ("bench_run", self.args.run_label.as_str()),
                ])
                .until_done()
                .await?;
            let billed = done.metadata().total_bytes_billed;
            let job_created = done.metadata().job_reference.is_some();
            let mut iter = done.read();
            let mut rows = 0u64;
            let mut convert = Duration::ZERO;
            while let Some(row) = iter.next().await.transpose()? {
                let start = Instant::now();
                let value: T = row.try_into()?;
                convert += start.elapsed();
                std::hint::black_box(&value);
                rows += 1;
            }
            Ok((rows, billed, convert, job_created))
        })
        .await?;
        let mut extra = serde_json::Map::new();
        extra.insert("from_row_secs".into(), convert.as_secs_f64().into());
        extra.insert("job_created".into(), job_created.into());
        Ok(Outcome {
            secs,
            rows,
            path: Some("rest_json_pages".into()),
            bytes_billed: billed,
            extra,
            ..Default::default()
        })
    }

    fn table_path(&self, table: &str) -> String {
        format!(
            "projects/{}/datasets/{}/tables/{table}",
            self.args.project, self.args.dataset
        )
    }

    /// Reads the scan table through Storage Read, with as many streams as the `bigquery`
    /// crate asks for by default and the same LZ4 buffers, decoding each stream's Arrow on
    /// its own task.
    async fn scan_arrow(&self) -> anyhow::Result<Outcome> {
        let (secs, (rows, wire, streams)) = timed(async || {
            let session = self
                .read
                .create_read_session()
                .set_parent(format!("projects/{}", self.args.project))
                .set_read_session(
                    ReadSession::new()
                        .set_data_format(DataFormat::Arrow)
                        .set_table(self.table_path(SCAN_TABLE))
                        .set_read_options(
                            TableReadOptions::new().set_arrow_serialization_options(
                                ArrowSerializationOptions::new()
                                    .set_buffer_compression(CompressionCodec::Lz4Frame),
                            ),
                        ),
                )
                .set_max_stream_count(parallelism() as i32)
                .send()
                .await?;
            let schema = session
                .arrow_schema()
                .ok_or_else(|| anyhow::anyhow!("no Arrow schema in the session"))?
                .serialized_schema
                .clone();
            let tasks: Vec<_> = session
                .streams
                .iter()
                .map(|stream| {
                    let read = self.read.clone();
                    let name = stream.name.clone();
                    let schema = schema.clone();
                    tokio::spawn(async move {
                        let mut decoder = StreamDecoder::new();
                        let mut schema_buf = Buffer::from(schema.to_vec());
                        anyhow::ensure!(decoder.decode(&mut schema_buf)?.is_none());
                        let mut responses = read.read_rows().set_read_stream(name).send().await?;
                        let (mut rows, mut wire) = (0u64, 0u64);
                        while let Some(response) = responses.next().await.transpose()? {
                            let Some(batch) = response.arrow_record_batch() else {
                                continue;
                            };
                            wire += batch.serialized_record_batch.len() as u64;
                            let mut buf = Buffer::from(batch.serialized_record_batch.to_vec());
                            while !buf.is_empty() {
                                if let Some(decoded) = decoder.decode(&mut buf)? {
                                    rows += decoded.num_rows() as u64;
                                    std::hint::black_box(&decoded);
                                }
                            }
                        }
                        anyhow::Ok((rows, wire))
                    })
                })
                .collect();
            let streams = tasks.len() as u64;
            let (mut rows, mut wire) = (0u64, 0u64);
            for task in tasks {
                let (r, w) = task.await??;
                rows += r;
                wire += w;
            }
            Ok((rows, wire, streams))
        })
        .await?;
        Ok(Outcome {
            secs,
            rows,
            bytes: Some(wire),
            path: Some("storage_read_raw_client_plus_arrow_ipc".into()),
            streams: Some(streams),
            ..Default::default()
        })
    }

    async fn write(&mut self) -> anyhow::Result<Outcome> {
        if self.write_rows.is_none() {
            self.write_rows = Some(write_rows());
        }
        let rows = self.write_rows.as_ref().expect("generated above");
        let table = self.table_path("write_official");
        let schema = Arc::new(arrow_schema());
        let (secs, (written, sent, requests)) = timed(async || {
            let generator = IpcDataGenerator {};
            let options = IpcWriteOptions::default();
            let mut tracker = DictionaryTracker::new(false);
            let encoded =
                generator.schema_to_bytes_with_dictionary_tracker(&schema, &mut tracker, &options);
            let mut schema_bytes = Vec::new();
            arrow_ipc::writer::write_message(&mut schema_bytes, encoded, &options)?;
            let writer = self
                .write
                .open_default_stream(table)
                .build_arrow(ArrowSchema::new().set_serialized_schema(schema_bytes.clone()))
                .await?;
            let writer = &writer;
            let schema = &schema;
            let results: Vec<anyhow::Result<(u64, u64)>> =
                futures::stream::iter(rows.chunks(WRITE_BATCH_ROWS))
                    .map(|chunk| async move {
                        let batch = to_arrow(schema, chunk)?;
                        let generator = IpcDataGenerator {};
                        let options = IpcWriteOptions::default();
                        let mut tracker = DictionaryTracker::new(false);
                        let (_, encoded) = generator.encode(
                            &batch,
                            &mut tracker,
                            &options,
                            &mut Default::default(),
                        )?;
                        let mut bytes = Vec::new();
                        arrow_ipc::writer::write_message(&mut bytes, encoded, &options)?;
                        let len = bytes.len() as u64;
                        writer
                            .append(ArrowRecordBatch::new().set_serialized_record_batch(bytes))
                            .send()
                            .await?;
                        anyhow::Ok((chunk.len() as u64, len))
                    })
                    .buffer_unordered(WRITE_WINDOW)
                    .collect()
                    .await;
            let (mut written, mut sent, mut requests) = (0u64, 0u64, 0u64);
            for result in results {
                let (r, b) = result?;
                written += r;
                sent += b + schema_bytes.len() as u64;
                requests += 1;
            }
            Ok((written, sent, requests))
        })
        .await?;
        anyhow::ensure!(written == rows.len() as u64, "{written} rows written");
        let mut extra = serde_json::Map::new();
        extra.insert("requests".into(), requests.into());
        extra.insert("window".into(), WRITE_WINDOW.into());
        extra.insert("batch_rows".into(), WRITE_BATCH_ROWS.into());
        Ok(Outcome {
            secs,
            rows: written,
            bytes: Some(sent),
            path: Some("storage_write_default_stream_arrow".into()),
            extra,
            ..Default::default()
        })
    }

    async fn run(&mut self, request: &Request) -> anyhow::Result<Outcome> {
        match request.scenario {
            Scenario::QueryConst => self.typed_query::<SmallRow>(SQL_CONST.to_string()).await,
            Scenario::Query1k => self.typed_query::<Row1k>(sql_1k()).await,
            Scenario::Query200kRows => self.typed_query::<Row200k>(sql_200k()).await,
            Scenario::ScanRows => {
                let sql = format!(
                    "SELECT * FROM `{}.{}.{SCAN_TABLE}`",
                    self.args.project, self.args.dataset
                );
                let mut outcome = self.typed_query::<ScanRow>(sql).await?;
                outcome.path = Some("rest_json_pages_select_star".into());
                Ok(outcome)
            }
            Scenario::ScanArrow => self.scan_arrow().await,
            Scenario::Write => self.write().await,
            Scenario::Query200kArrow => anyhow::bail!(
                "n/a: the query client returns JSON rows only; it has no Arrow result path"
            ),
            Scenario::Decode => anyhow::bail!(
                "n/a: FromRow converts the JSON rows of a live query (Row has no public \
                 constructor), and there is no Arrow to struct decoder"
            ),
        }
    }
}

/// The scan table's columns as BigQuery's Storage Write expects them in Arrow: NUMERIC as
/// `Decimal128(38, 9)`, DATE as `Date32` and TIMESTAMP as microseconds in UTC.
fn arrow_schema() -> Schema {
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

/// The NUMERIC value as the unscaled `i128` of a scale-9 decimal.
fn decimal9(d: rust_decimal::Decimal) -> i128 {
    let mut d = d;
    d.rescale(9);
    d.mantissa()
}

fn days(d: jiff::civil::Date) -> i32 {
    (d - jiff::civil::date(1970, 1, 1)).get_days()
}

/// Builds one Arrow batch from plain rows, the step a caller of the official Arrow writer has
/// to write themselves.
fn to_arrow(schema: &Arc<Schema>, rows: &[PlainRow]) -> anyhow::Result<RecordBatch> {
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse()?;
    let bq = BigQuery::builder()
        .with_project_id(args.project.clone())
        .build()
        .await?;
    let read = Read::builder().build().await?;
    let write = Write::builder().build().await?;
    let mut official = Official {
        bq,
        read,
        write,
        args,
        write_rows: None,
    };
    let info = serde_json::json!({
        "client": "official",
        "crate": "google-cloud-bigquery",
        "settings": format!(
            "client defaults; scans: Storage Read raw client, max_stream_count = {} (same as \
             the bigquery crate's default), LZ4, one task per stream; writes: Arrow on the \
             default stream, {WRITE_BATCH_ROWS} rows per request, {WRITE_WINDOW} requests in \
             flight (same window as the bigquery crate's default)",
            parallelism()
        ),
    });
    serve(info, async |r| official.run(r).await).await
}
