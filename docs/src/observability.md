# Observability

The library uses [tracing](https://github.com/tokio-rs/tracing). Every call opens one span at the
`DEBUG` level, and the spans carry what a call used besides how long it took: bytes processed
and billed, slot milliseconds, rows and bytes read or sent, etc.

The spans are:

- `BigQuery Query`: one query terminal call, `query()`, `execute()`, `dry_run()` etc.;
- `BigQuery Read`: one table read through the Storage Read API, including the read of a large
  query result;
- `BigQuery streaming write`: one writer, from `insert()`, `create_streaming_writer` or
  `create_cdc_writer`;
- `BigQuery commit write streams`: one `commit_write_streams` call;
- `BigQuery Cancel Job`, and the admin spans `BigQuery dataset`, `BigQuery datasets`,
  `BigQuery table`, `BigQuery tables`, `BigQuery job`, `BigQuery jobs` and `BigQuery schema`,
  which carry only the resource they work on.

Retries are logged at `WARN` inside the span of the call that retried.

## Showing the spans

Any tracing subscriber works. With `tracing-subscriber`, an `EnvFilter` that enables `DEBUG` for
the library and span close events, you see every span with its fields when it ends:

```rust,no_run
use bigquery::*;
use tracing_subscriber::fmt::format::FmtSpan;

# async fn example() -> BigQueryResult<()> {
tracing_subscriber::fmt()
    .with_env_filter("info,bigquery=debug")
    .with_span_events(FmtSpan::CLOSE)
    .init();

let db = BigQueryDb::new("my-gcp-project-id").await?;
let outcome = db
    .fluent()
    .query("SELECT word FROM `bigquery-public-data.samples.shakespeare` LIMIT 10")
    .execute()
    .await?;
# let _ = outcome;
# Ok(())
# }
```

Full example available [here](https://github.com/abdolence/bigquery-rs/blob/master/examples/job-stats-tracing.rs).

## OpenTelemetry

With [tracing-opentelemetry](https://github.com/tokio-rs/tracing-opentelemetry) every span field
becomes a span attribute with the same name, such as `/bigquery/bytes_billed`, so you can see the
cost of a request next to its latency in Cloud Trace, Jaeger, etc.

The fields are declared empty when the span opens and recorded once their value is known, which
`tracing-opentelemetry` exports the same way as fields given at the start. Add its layer to your
subscriber as usual:

```rust,ignore
use opentelemetry::trace::TracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
    .with_batch_exporter(exporter) // any exporter, e.g. opentelemetry-otlp
    .build();

tracing_subscriber::registry()
    .with(tracing_subscriber::EnvFilter::new("info,bigquery=debug"))
    .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("my-service")))
    .init();
```

Be aware the library spans are `DEBUG`, so your filter has to enable that level for `bigquery`.

## Common rules for the fields

- A figure BigQuery did not report is left unrecorded, never recorded as `0`. The two estimates of
  a read session are the exception: BigQuery sends them as plain numbers, with no way to tell `0`
  from unset.
- The library makes no call only for the stats. Every figure comes from a response the call
  receives anyway, or is counted locally.
- No span field carries the SQL text, a parameter value or a row filter. The query span has the
  length of the SQL only.
- A row that `stream_query()` or a table read skips is logged with the kind of error, the row and
  the field path, without the message, which can contain the cell's text. A retry is logged with
  BigQuery's error message, as BigQuery wrote it.

## Query span

`BigQuery Query` covers a query from the `Query` call until the library knows where the rows are.
It does not cover streaming the rows: a result read through Storage Read has its own
`BigQuery Read` span, a sibling of the query span under your current span.

| Field | What it means | Where it comes from |
|---|---|---|
| `/bigquery/sql_len` | the length of the SQL text in bytes | the SQL, when the span opens |
| `/bigquery/job_id` | the job that ran the query; unset for a query BigQuery ran without a job | the job reference of the `Query` response |
| `/bigquery/query_id` | the ID BigQuery gave the query, with or without a job | the `Query` response |
| `/bigquery/location` | where the query ran | the job reference, otherwise the location of the `Query` response |
| `/bigquery/statement_type` | `SELECT`, `INSERT`, `UPDATE`, `CREATE_TABLE`, etc. | the `Query` response, otherwise the job's query statistics |
| `/bigquery/bytes_processed` | bytes the query processed | the `Query` response, `GetQueryResults`, then the job's statistics |
| `/bigquery/bytes_billed` | bytes billed, after BigQuery's rounding and minimums | the `Query` response, then the job's query statistics |
| `/bigquery/slot_ms` | slot milliseconds the query used | the `Query` response, then the job's statistics |
| `/bigquery/cache_hit` | whether the query cache answered | the `Query` response, `GetQueryResults`, then the job's query statistics |
| `/bigquery/dml_rows` | rows a DML statement changed | the same three as `cache_hit` |
| `/bigquery/total_rows` | rows in the result | the `Query` response, then `GetQueryResults` |
| `/bigquery/route` | where the rows came from: `inline`, `storage_read` or `none` | the library, for the terminals that read rows |

Each figure is taken from the first response that reports it, in the order of the table. Which of
them a query can have depends on its route:

- **Inline**, a result complete in the first response: only the `Query` response's own figures,
  since there is no further call on this route. In the live tests BigQuery reported bytes
  processed, bytes billed, slot milliseconds and cache hit there;
- **Storage Read**, a larger result: the `Query` response, and then the statistics of the job that
  the library reads anyway to find the destination table;
- **Polled**, a job that was not complete in the first response: `GetQueryResults` and the job's
  statistics. `GetQueryResults` has no bytes billed and no slot milliseconds, so those come from
  the job only;
- **`execute()`**: the same as inline when the job completes in the first response. `route` is not
  recorded, since `execute()` reads no rows;
- **`dry_run()`**: none of the figures. Its estimate is in `BigQueryDryRunResult` instead.

The same figures are in `BigQueryJobStats` from `query_with_stats()`, `stream_query_with_stats()`
and `execute()`, see [Job stats](./queries.md#job-stats).

`slot_ms` is not `0` even for a query that reads no table. On 1,000 generated rows BigQuery
reported 0 bytes processed and billed, and 25 slot ms inline, 152 slot ms through Storage Read.

## Read span

`BigQuery Read` covers one read session, from opening it until the stream of rows or batches is
dropped.

| Field | What it means | Where it comes from |
|---|---|---|
| `/bigquery/table` | the table being read, `project.dataset.table` or `dataset.table` | the read, when the span opens |
| `/bigquery/streams` | the read streams BigQuery gave the session | the read session |
| `/bigquery/estimated_bytes_scanned` | BigQuery's estimate of the bytes the session scans | the read session, as BigQuery sends it |
| `/bigquery/estimated_rows` | BigQuery's estimate of the rows the session returns | the read session, as BigQuery sends it |
| `/bigquery/rows_read` | rows received over every stream | the sum of the row counts of every `ReadRows` response |
| `/bigquery/bytes_read` | uncompressed bytes received | the sum of the uncompressed sizes the `ReadRows` responses report, when any of them does |
| `/bigquery/throttle_percent` | how much BigQuery throttled the read, 0 to 100 | the highest throttle state any `ReadRows` response reported, when any of them does |

The three running totals are recorded when the stream is dropped, which also happens when it ends
or fails. A read you drop early records what it got so far.

The number of streams is what BigQuery gave, which can be fewer than the library asked for. In the
benchmarks the library asked for 16 and got 4 for a 1M-row table.

## Write span

`BigQuery streaming write` covers one writer, from opening its write stream until its background
task ends. `insert()` and the CDC writer go through the same writer, so they have the same span.

| Field | What it means | Where it comes from |
|---|---|---|
| `/bigquery/table` | the table being written | the writer, when the span opens |
| `/bigquery/write_mode` | `Default`, `Committed`, `Pending` or `Buffered` | the writer's options, when the span opens |
| `/bigquery/rows_appended` | rows in the batches BigQuery acknowledged | counted by the writer, the same as `rows_written` in `BigQueryWriteSummary` |
| `/bigquery/bytes_sent` | the encoded size of every `AppendRows` request sent, resends included, before gRPC framing | counted by the writer, the same as `bytes_sent` in `BigQueryWriteSummary` |
| `/bigquery/appends` | `AppendRows` requests sent, resends included | counted by the writer |
| `/bigquery/retries` | requests that sent a batch again | counted by the writer |

The four counts are recorded when the writer's background task ends: after `finish()` or
`finalize()`, when the writer is dropped, and when it fails for good.

`BigQuery commit write streams` has only `/bigquery/table`; the commit time is in the result of the
commit.
