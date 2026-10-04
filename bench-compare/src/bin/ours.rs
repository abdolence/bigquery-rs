//! The `bigquery` crate's contender, plus the setup, teardown and billing steps of a run,
//! which use this crate because it is the client the harness already trusts.
//!
//! `ours --project P --dataset D --run-label L [setup|teardown|billing|serve]`

use bench_compare::*;
use bigquery::arrow_array::RecordBatch;
use bigquery::*;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Deserialize)]
#[allow(dead_code, reason = "decoded to be timed, never read")]
struct SmallRow {
    x: i64,
}

#[derive(Deserialize)]
#[allow(dead_code, reason = "decoded to be timed, never read")]
struct Row1k {
    x: i64,
    s: String,
}

#[derive(Deserialize)]
#[allow(dead_code, reason = "decoded to be timed, never read")]
struct Row200k {
    x: i64,
    s: String,
    f: f64,
    b: bool,
}

#[derive(Serialize, Deserialize)]
struct Inner {
    a: i64,
    b: String,
}

#[derive(Serialize, Deserialize)]
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
    n1: BigQueryDecimal<rust_decimal::Decimal>,
    n2: BigQueryDecimal<rust_decimal::Decimal>,
    d1: BigQueryDate,
    d2: BigQueryDate,
    t1: BigQueryTimestamp,
    t2: BigQueryTimestamp,
    st: Inner,
    arr: Vec<i64>,
}

impl From<PlainRow> for ScanRow {
    fn from(r: PlainRow) -> Self {
        Self {
            id: r.id,
            i1: r.i1,
            i2: r.i2,
            i3: r.i3,
            f1: r.f1,
            f2: r.f2,
            f3: r.f3,
            s1: r.s1,
            s2: r.s2,
            s3: r.s3,
            b1: r.b1,
            b2: r.b2,
            n1: BigQueryDecimal(r.n1),
            n2: BigQueryDecimal(r.n2),
            d1: BigQueryDate(r.d1),
            d2: BigQueryDate(r.d2),
            t1: BigQueryTimestamp(r.t1),
            t2: BigQueryTimestamp(r.t2),
            st: Inner {
                a: r.st_a,
                b: r.st_b,
            },
            arr: r.arr,
        }
    }
}

/// The route and stream count the crate records on its query and read spans, so a result can
/// say which path its rows took. Only those two span names are enabled, and no events, so the
/// capture adds a span per call and nothing per row.
#[derive(Default, Clone)]
struct SpanFields(Arc<Mutex<(Option<String>, Option<u64>)>>);

impl SpanFields {
    fn take(&self) -> (Option<String>, Option<u64>) {
        std::mem::take(&mut *self.0.lock().expect("not poisoned"))
    }
}

struct FieldVisitor<'a>(&'a Mutex<(Option<String>, Option<u64>)>);

impl Visit for FieldVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "/bigquery/route" {
            self.0.lock().expect("not poisoned").0 = Some(value.to_string());
        }
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "/bigquery/streams" {
            self.0.lock().expect("not poisoned").1 = Some(value);
        }
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        if field.name() == "/bigquery/streams" {
            self.0.lock().expect("not poisoned").1 = u64::try_from(value).ok();
        }
    }
    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

impl<S: tracing::Subscriber> Layer<S> for SpanFields {
    fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
        attrs.record(&mut FieldVisitor(&self.0));
    }
    fn on_record(&self, _id: &Id, values: &Record<'_>, _ctx: Context<'_, S>) {
        values.record(&mut FieldVisitor(&self.0));
    }
}

struct Ours {
    db: BigQueryDb,
    args: Args,
    location: BigQueryLocation,
    /// Sends `.job_creation_required()` on the scenario queries, to measure what a job costs.
    job_required: bool,
    spans: SpanFields,
    write_rows: Option<Vec<ScanRow>>,
    decode_batches: Option<Vec<RecordBatch>>,
}

impl Ours {
    fn dataset(&self) -> anyhow::Result<BigQueryDatasetRef> {
        Ok(BigQueryDatasetRef::new(
            &self.args.project,
            BigQueryDatasetId::new(&self.args.dataset)?,
        )?)
    }

    fn table(&self, name: &str) -> anyhow::Result<BigQueryTableRef> {
        Ok(self.dataset()?.table(name.parse()?))
    }

    fn query(&self, sql: impl Into<String>) -> BigQueryQueryBuilder<'_, BigQueryDb> {
        let query = self.base_query(sql);
        if self.job_required {
            query.job_creation_required()
        } else {
            query
        }
    }

    fn base_query(&self, sql: impl Into<String>) -> BigQueryQueryBuilder<'_, BigQueryDb> {
        self.db
            .fluent()
            .query(sql)
            .use_query_cache(false)
            .location(self.location.clone())
            .label("bench_client", "ours")
            .label("bench_run", self.args.run_label.as_str())
    }

    async fn typed_query<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        sql: String,
    ) -> anyhow::Result<Outcome> {
        self.spans.take();
        let (secs, (rows, stats)) =
            timed(async || Ok(self.query(sql).obj::<T>().query_with_stats().await?)).await?;
        let (path, streams) = self.spans.take();
        Ok(Outcome {
            secs,
            rows: rows.len() as u64,
            path,
            streams,
            bytes_billed: stats.total_bytes_billed,
            extra: [("job_created".to_string(), stats.job.is_some().into())]
                .into_iter()
                .collect(),
            ..Default::default()
        })
    }

    async fn run(&mut self, request: &Request) -> anyhow::Result<Outcome> {
        match request.scenario {
            Scenario::QueryConst => self.typed_query::<SmallRow>(SQL_CONST.to_string()).await,
            Scenario::Query1k => self.typed_query::<Row1k>(sql_1k()).await,
            Scenario::Query200kRows => self.typed_query::<Row200k>(sql_200k()).await,
            Scenario::Query200kArrow => self.query_200k_arrow().await,
            Scenario::ScanRows => self.scan_rows().await,
            Scenario::ScanArrow => self.scan_arrow().await,
            Scenario::Write => self.write().await,
            Scenario::Decode => self.decode().await,
        }
    }

    async fn query_200k_arrow(&self) -> anyhow::Result<Outcome> {
        self.spans.take();
        let (secs, rows) = timed(async || {
            let mut stream = self.query(sql_200k()).record_batches().await?;
            let mut rows = 0u64;
            while let Some(batch) = stream.try_next().await? {
                rows += batch.num_rows() as u64;
            }
            Ok(rows)
        })
        .await?;
        let (path, streams) = self.spans.take();
        Ok(Outcome {
            secs,
            rows,
            path,
            streams,
            ..Default::default()
        })
    }

    async fn scan_rows(&self) -> anyhow::Result<Outcome> {
        let table = self.table(SCAN_TABLE)?;
        self.spans.take();
        let (secs, rows) = timed(async || {
            let mut stream = self
                .db
                .fluent()
                .select()
                .from(table)
                .obj::<ScanRow>()
                .stream_query_with_errors()
                .await?;
            let mut rows = 0u64;
            while let Some(row) = stream.try_next().await? {
                std::hint::black_box(&row);
                rows += 1;
            }
            Ok(rows)
        })
        .await?;
        let (_, streams) = self.spans.take();
        Ok(Outcome {
            secs,
            rows,
            path: Some("storage_read".into()),
            streams,
            ..Default::default()
        })
    }

    async fn scan_arrow(&self) -> anyhow::Result<Outcome> {
        let table = self.table(SCAN_TABLE)?;
        self.spans.take();
        let (secs, (rows, bytes)) = timed(async || {
            let mut stream = self
                .db
                .fluent()
                .select()
                .from(table)
                .record_batches()
                .await?;
            let (mut rows, mut bytes) = (0u64, 0u64);
            while let Some(batch) = stream.try_next().await? {
                rows += batch.num_rows() as u64;
                bytes += batch.get_array_memory_size() as u64;
            }
            Ok((rows, bytes))
        })
        .await?;
        let (_, streams) = self.spans.take();
        let mut extra = serde_json::Map::new();
        extra.insert("arrow_memory_bytes".into(), bytes.into());
        Ok(Outcome {
            secs,
            rows,
            path: Some("storage_read".into()),
            streams,
            extra,
            ..Default::default()
        })
    }

    async fn write(&mut self) -> anyhow::Result<Outcome> {
        if self.write_rows.is_none() {
            self.write_rows = Some(write_rows().into_iter().map(ScanRow::from).collect());
        }
        let rows = self.write_rows.as_ref().expect("generated above");
        let table = self.table("write_ours")?;
        let (secs, summary) = timed(async || {
            Ok(self
                .db
                .fluent()
                .insert()
                .into(table)
                .objects(rows)
                .execute()
                .await?)
        })
        .await?;
        anyhow::ensure!(
            summary.rows_written == rows.len() as u64 && summary.rows_failed == 0,
            "{summary:?}"
        );
        let mut extra = serde_json::Map::new();
        extra.insert("requests".into(), summary.batches.into());
        Ok(Outcome {
            secs,
            rows: summary.rows_written,
            bytes: Some(summary.bytes_sent),
            path: Some("storage_write_default_stream_proto".into()),
            extra,
            ..Default::default()
        })
    }

    async fn decode(&mut self) -> anyhow::Result<Outcome> {
        if self.decode_batches.is_none() {
            let table = self.table(SCAN_TABLE)?;
            let batches: Vec<RecordBatch> = self
                .db
                .fluent()
                .select()
                .from(table)
                .record_batches()
                .await?
                .try_collect()
                .await?;
            self.decode_batches = Some(batches);
        }
        let batches = self.decode_batches.as_ref().expect("read above");
        let (secs, rows) = timed(async || {
            let mut rows = 0u64;
            let mut failed = None;
            for batch in batches {
                __bench_decode_each(batch, |row: BigQueryResult<ScanRow>| match row {
                    Ok(row) => {
                        std::hint::black_box(&row);
                        rows += 1;
                    }
                    Err(err) => failed = Some(err),
                });
            }
            match failed {
                Some(err) => Err(err.into()),
                None => Ok(rows),
            }
        })
        .await?;
        Ok(Outcome {
            secs,
            rows,
            path: Some("in_memory_arrow_to_struct_single_thread".into()),
            ..Default::default()
        })
    }

    async fn setup(&self) -> anyhow::Result<serde_json::Value> {
        let ds = format!("`{}.{}`", self.args.project, self.args.dataset);
        let mut billed = 0i64;
        for sql in [
            format!(
                "CREATE SCHEMA {ds} OPTIONS (location = '{}', default_table_expiration_days = 1)",
                self.args.location
            ),
            format!("CREATE TABLE {ds}.{SCAN_TABLE} AS {}", scan_select()),
            format!("CREATE TABLE {ds}.write_ours LIKE {ds}.{SCAN_TABLE}"),
            format!("CREATE TABLE {ds}.write_official LIKE {ds}.{SCAN_TABLE}"),
        ] {
            let outcome = self
                .query(sql)
                .job_timeout(std::time::Duration::from_secs(600))
                .execute()
                .await?;
            billed += outcome.total_bytes_billed.unwrap_or(0);
        }
        let scan = self
            .db
            .fluent()
            .schema()
            .table(self.table(SCAN_TABLE)?)
            .get()
            .await?;
        Ok(serde_json::json!({
            "setup_bytes_billed": billed,
            "scan_table_rows": scan.num_rows,
            "scan_table_logical_bytes": scan.num_bytes,
        }))
    }

    async fn teardown(&self) -> anyhow::Result<serde_json::Value> {
        let mut tables = serde_json::Map::new();
        for name in ["write_ours", "write_official"] {
            if let Ok(table) = self
                .db
                .fluent()
                .schema()
                .table(self.table(name)?)
                .get()
                .await
            {
                tables.insert(
                    name.into(),
                    serde_json::json!({ "rows": table.num_rows, "logical_bytes": table.num_bytes }),
                );
            }
        }
        self.db
            .fluent()
            .schema()
            .dataset(self.dataset()?)
            .dangerously_delete_with_contents()
            .await?;
        Ok(serde_json::json!({ "deleted": self.args.dataset, "write_tables": tables }))
    }

    /// Times a small authenticated call, `datasets.get` over the v2 gRPC channel, eleven times
    /// on one channel; the first includes the connection and token set-up, the rest are the
    /// round trip plus BigQuery's handling of a metadata read.
    async fn ping(&self) -> anyhow::Result<serde_json::Value> {
        let mut secs = Vec::new();
        for _ in 0..11 {
            let (s, _) = timed(async || {
                Ok(self
                    .db
                    .fluent()
                    .schema()
                    .dataset(self.dataset()?)
                    .get()
                    .await?)
            })
            .await?;
            secs.push(s);
        }
        Ok(serde_json::json!({ "call": "datasets.get (gRPC)", "secs": secs }))
    }

    /// The bytes processed and billed by every job of this run, per client label.
    async fn billing(&self) -> anyhow::Result<serde_json::Value> {
        #[derive(Deserialize, Serialize)]
        struct Billing {
            client: Option<String>,
            jobs: i64,
            bytes_processed: Option<i64>,
            bytes_billed: Option<i64>,
        }
        let sql = format!(
            "SELECT (SELECT value FROM UNNEST(labels) WHERE key = 'bench_client') AS client, \
               COUNT(*) AS jobs, SUM(total_bytes_processed) AS bytes_processed, \
               SUM(total_bytes_billed) AS bytes_billed \
             FROM `region-{}`.INFORMATION_SCHEMA.JOBS_BY_USER \
             WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) \
               AND EXISTS (SELECT 1 FROM UNNEST(labels) WHERE key = 'bench_run' AND value = '{}') \
             GROUP BY client ORDER BY client",
            self.args.location,
            self.args.run_label.replace(['\'', '\\'], "")
        );
        let rows: Vec<Billing> = self
            .db
            .fluent()
            .query(sql)
            .location(self.location.clone())
            .obj::<Billing>()
            .query()
            .await?;
        Ok(serde_json::to_value(rows)?)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let spans = SpanFields::default();
    tracing_subscriber::registry()
        .with(
            spans
                .clone()
                .with_filter(tracing_subscriber::filter::filter_fn(|m| {
                    m.is_span() && (m.name() == "BigQuery Query" || m.name() == "BigQuery Read")
                })),
        )
        .init();
    let mut args = Args::parse()?;
    let job_required = args.rest.iter().any(|a| a == "--job-creation-required");
    args.rest.retain(|a| a != "--job-creation-required");
    let db = BigQueryDb::new(&args.project).await?;
    let mut ours = Ours {
        db,
        location: BigQueryLocation::new(&args.location)?,
        job_required,
        args: args.clone(),
        spans,
        write_rows: None,
        decode_batches: None,
    };
    let print = |v: serde_json::Value| println!("{v}");
    match args.rest.first().map(String::as_str) {
        Some("setup") => print(ours.setup().await?),
        Some("teardown") => print(ours.teardown().await?),
        Some("billing") => print(ours.billing().await?),
        Some("ping") => print(ours.ping().await?),
        None | Some("serve") => {
            let info = serde_json::json!({
                "client": if job_required { "ours_required" } else { "ours" },
                "job_creation": if job_required { "required" } else { "optional (default)" },
                "crate": "bigquery",
                "storage_read_max_streams_default": parallelism(),
                "settings": "defaults: Storage Read max_stream_count = available parallelism, LZ4; \
                             writes: default stream, max_request_bytes 19 MB, 8 requests in flight",
            });
            serve(info, async |r| ours.run(r).await).await?;
        }
        Some(other) => anyhow::bail!("unknown command {other}"),
    }
    Ok(())
}
