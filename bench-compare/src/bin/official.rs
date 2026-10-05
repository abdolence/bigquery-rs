//! Google's official Rust client, `google-cloud-bigquery`, as a contender.
//!
//! Queries go through its `BigQuery` client, whose rows come back as JSON pages of
//! `jobs.getQueryResults` over REST and convert to structs with `#[derive(FromRow)]`. Table
//! scans go through its raw Storage Read client, which hands out Arrow IPC bytes; decoding
//! them is left to the caller, so the harness does it with `arrow-ipc` as a user would. Writes
//! go through its Arrow writer on the default stream, from rows or from ready Arrow batches.
//!
//! `official --project P --dataset D --run-label L`

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::Schema;
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
    write_batches: Option<Vec<RecordBatch>>,
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

    /// Writes the rows as Arrow built from them inside the timed region, the work a caller of
    /// the official Arrow writer does when starting from Rust values.
    async fn write(&mut self) -> anyhow::Result<Outcome> {
        if self.write_rows.is_none() {
            self.write_rows = Some(write_rows());
        }
        let rows = self.write_rows.as_ref().expect("generated above");
        let schema = Arc::new(PlainRow::arrow_schema());
        let batches = rows
            .chunks(WRITE_BATCH_ROWS)
            .map(|chunk| PlainRow::record_batch(&schema, chunk));
        self.append_arrow("write_official", &schema, batches, rows.len())
            .await
    }

    /// Writes record batches built before the timed region, so the time is the IPC encoding
    /// and the appends only.
    async fn write_arrow(&mut self) -> anyhow::Result<Outcome> {
        if self.write_batches.is_none() {
            self.write_batches = Some(write_batches()?);
        }
        let batches = self.write_batches.as_ref().expect("built above");
        let schema = Arc::new(PlainRow::arrow_schema());
        let rows = batches.iter().map(RecordBatch::num_rows).sum();
        self.append_arrow(
            "write_official_arrow",
            &schema,
            batches.iter().map(|batch| Ok(batch.clone())),
            rows,
        )
        .await
    }

    /// Appends every batch of `batches` to the default stream of `table`, one request each,
    /// [`WRITE_WINDOW`] requests in flight. The timed region starts before the first batch is
    /// taken from `batches`, so whatever producing a batch costs is part of the time.
    async fn append_arrow(
        &self,
        table: &str,
        schema: &Arc<Schema>,
        batches: impl Iterator<Item = anyhow::Result<RecordBatch>>,
        expected_rows: usize,
    ) -> anyhow::Result<Outcome> {
        let table = self.table_path(table);
        let (secs, (written, sent, requests)) = timed(async || {
            let generator = IpcDataGenerator {};
            let options = IpcWriteOptions::default();
            let mut tracker = DictionaryTracker::new(false);
            let encoded =
                generator.schema_to_bytes_with_dictionary_tracker(schema, &mut tracker, &options);
            let mut schema_bytes = Vec::new();
            arrow_ipc::writer::write_message(&mut schema_bytes, encoded, &options)?;
            let writer = self
                .write
                .open_default_stream(table)
                .build_arrow(ArrowSchema::new().set_serialized_schema(schema_bytes.clone()))
                .await?;
            let writer = &writer;
            let results: Vec<anyhow::Result<(u64, u64)>> = futures::stream::iter(batches)
                .map(|batch| async move {
                    let batch = batch?;
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
                    anyhow::Ok((batch.num_rows() as u64, len))
                })
                .buffer_unordered(WRITE_WINDOW)
                .collect()
                .await;
            let (mut written, mut sent, mut requests) = (0u64, 0u64, 0u64);
            for result in results {
                let (rows, batch_bytes) = result?;
                written += rows;
                sent += batch_bytes + schema_bytes.len() as u64;
                requests += 1;
            }
            Ok((written, sent, requests))
        })
        .await?;
        anyhow::ensure!(written == expected_rows as u64, "{written} rows written");
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
            Scenario::WriteArrow => self.write_arrow().await,
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
        write_batches: None,
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
