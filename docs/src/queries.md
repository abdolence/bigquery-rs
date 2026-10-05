# Queries

The library runs GoogleSQL queries through the v2 `Query` call over gRPC and reads the results as
your own structures with serde, or as Arrow record batches. When to query and when to read the
table instead, see [Table reads or queries](./table-reads-or-queries.md).

```rust,no_run
use bigquery::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct WordCount {
    word: String,
    word_count: i64,
}

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let words: Vec<WordCount> = db
    .fluent()
    .query(
        "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` \
         WHERE corpus = @corpus AND word_count >= @min_count \
         ORDER BY word_count DESC LIMIT 10",
    )
    .param("corpus", "hamlet")
    .param("min_count", 100)
    .obj::<WordCount>()
    .query()
    .await?;
println!("{words:?}");
# Ok(())
# }
```

Columns map to the fields of your structure by name, with the types described in
[Type mapping](./types.md). The target type is `DeserializeOwned + Send + 'static`, since the rows of a
large result are decoded on each read stream's task.

## Reading the results

`.obj::<T>()` has these terminals:

- `query()`: every row in a `Vec`, failing on the first error;
- `stream_query_with_errors()`: a stream of `BigQueryResult<T>`. A row that fails to decode is
  one `Err` item and the stream goes on; a read stream that fails for good is one `Err` and then
  the stream ends;
- `stream_query()`: a stream of `T`. Failures are logged at `error!` and skipped, so a stream
  that ended does not mean every row was read. Use `stream_query_with_errors()` if you need to
  tell the two apart;
- `query_with_stats()` and `stream_query_with_stats()`: the same as `query()` and
  `stream_query_with_errors()`, with what the job used, see [Job stats](#job-stats).

```rust,no_run
use bigquery::*;
use futures::TryStreamExt;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct WordCount {
    word: String,
    word_count: i64,
}

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let mut words = db
    .fluent()
    .query("SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare`")
    .obj::<WordCount>()
    .stream_query_with_errors()
    .await?;

while let Some(word) = words.try_next().await? {
    println!("{}: {}", word.word, word.word_count);
}
# Ok(())
# }
```

The query terminal waits for the job to finish before the first row streams. Dropping the stream
stops the reading, and does not cancel the job, see [Cancellation](#cancellation).

Rows of a large result come from several read streams at once, so they arrive in no particular
order, even with `ORDER BY`. A result that comes inline keeps its order. If the order matters for a
large result, sort the rows on your side, or ask for one read stream with
`.read_options(BigQueryReadOptions::new().with_max_stream_count(1))`. I think one stream keeps the
order of the result, Google's Python client does the same for `ORDER BY` queries, but the library
does not test it.

To get the result as Arrow, without serde, use `record_batches()` directly on the query:

```rust,no_run
use bigquery::*;
use futures::TryStreamExt;

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let mut batches = db
    .fluent()
    .query("SELECT corpus, COUNT(*) AS words FROM `bigquery-public-data.samples.shakespeare` GROUP BY corpus")
    .record_batches()
    .await?;

while let Some(batch) = batches.try_next().await? {
    println!("{} rows, schema {:?}", batch.num_rows(), batch.schema());
}
# Ok(())
# }
```

`RecordBatch` is `arrow_array::RecordBatch`, re-exported as `bigquery::arrow_array`.

## Parameters

Values go into the query only as query parameters, which BigQuery binds on its side. The library
never writes a value into the SQL text, so a value with quotes, backticks, comments or `@x` in it
is just a value. A corpus of such values is tested on all the parameter forms below, as STRING,
ARRAY, STRUCT and JSON, against a fake server. Against BigQuery itself a few injection payloads
are tested as STRING parameters.

Be aware this protects only the values: SQL you build with `format!` from untrusted input is still
your SQL.

### Named parameters

`.param(name, value)` adds `@name`, with the type inferred from the value's serde form:

- integers are INT64, floats FLOAT64, `bool` BOOL;
- strings and `char` are STRING, bytes (`serde_bytes`) BYTES, unit enum variants STRING;
- sequences are ARRAY of the first element's type;
- structures and string-keyed maps are STRUCT, with the fields in order;
- the library's wrappers are their own types: `BigQueryTimestamp`, `BigQueryDate`, `BigQueryTime`,
  `BigQueryDateTime`, `BigQueryJson`, `BigQueryInterval`, `BigQueryRange`, and `BigQueryDecimal`
  as NUMERIC, or BIGNUMERIC when the value does not fit NUMERIC.

```rust,no_run
use bigquery::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct Window {
    earliest: BigQueryTimestamp,
    latest: BigQueryTimestamp,
}

#[derive(Debug, Deserialize)]
struct Order {
    id: i64,
    city: String,
}

# async fn example(db: BigQueryDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let window = Window {
    earliest: BigQueryTimestamp("2026-10-01T00:00:00Z".parse()?),
    latest: BigQueryTimestamp("2026-10-02T00:00:00Z".parse()?),
};

let orders: Vec<Order> = db
    .fluent()
    .query(
        "SELECT id, city FROM shop.orders \
         WHERE city IN UNNEST(@cities) AND placed_at BETWEEN @window.earliest AND @window.latest \
         AND total >= @min_total",
    )
    .param("cities", vec!["Malmö", "Lund"])
    .param("window", window)
    .param("min_total", BigQueryDecimal("99.90"))
    .obj::<Order>()
    .query()
    .await?;
# let _ = orders;
# Ok(())
# }
```

Some values have no type to infer: `None`, an empty sequence, elements of different types. And a
plain `jiff` value serializes as text, so `.param` sends it as STRING. For these use
`.param_as(name, type, value)`, which takes the type and accepts the value in any form the
library writes for that type. `None` is a NULL of that type.

```rust,no_run
use bigquery::*;

# async fn example(db: BigQueryDb, since: Option<jiff::Timestamp>) -> BigQueryResult<()> {
let outcome = db
    .fluent()
    .query(
        "SELECT COUNT(*) FROM shop.orders \
         WHERE (@since IS NULL OR placed_at >= @since) \
         AND id NOT IN UNNEST(@excluded_ids) \
         AND ST_DWITHIN(location, @store, 10000)",
    )
    .param_as("since", BigQueryFieldType::Timestamp, since)
    .param_as("excluded_ids", BigQueryParamType::array_of(BigQueryFieldType::Int64), Vec::<i64>::new())
    .param_as("store", BigQueryFieldType::Geography, "POINT(13.0 55.6)")
    .execute()
    .await?;
# let _ = outcome;
# Ok(())
# }
```

`.params(&value)` adds every top-level field of a structure or string-keyed map as a named
parameter, inferred as by `.param`:

```rust,no_run
use bigquery::*;
use serde::Serialize;

#[derive(Serialize)]
struct Filter {
    city: String,
    min_total: f64,
}

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let filter = Filter {
    city: "Malmö".to_string(),
    min_total: 100.0,
};
let outcome = db
    .fluent()
    .query("DELETE FROM shop.orders WHERE city = @city AND total < @min_total")
    .params(&filter)
    .execute()
    .await?;
# let _ = outcome;
# Ok(())
# }
```

Parameter names must be GoogleSQL identifiers: ASCII letters, digits and `_`, not starting with a
digit.

### Positional parameters

`.positional_param(value)` and `.positional_param_as(type, value)` add the next `?`, inferred or
typed the same way as the named ones:

```rust,no_run
use bigquery::*;

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let outcome = db
    .fluent()
    .query("UPDATE shop.orders SET status = ? WHERE id = ?")
    .positional_param("shipped")
    .positional_param(42)
    .execute()
    .await?;
# let _ = outcome;
# Ok(())
# }
```

Named and positional parameters cannot be mixed in one query.

Full example available [here](https://github.com/abdolence/bigquery-rs/blob/master/examples/query.rs).

### Errors

The builder methods never fail. A parameter that cannot be encoded is kept in the builder, and the
terminal returns its error, `InvalidParametersError` or `SerializeError`, without sending
anything.

## Job settings

The query builder also has:

- `.location(..)`: where the job runs, see [Locations](./getting-started.md#locations);
- `.default_dataset(..)`: the dataset unqualified table names resolve in, a `BigQueryDatasetId` in
  the client's project or a `BigQueryDatasetRef` for another project;
- `.label(key, value)` and `.labels(..)`: labels on the job;
- `.maximum_bytes_billed(bytes)`: the job fails without running if it would bill more;
- `.use_query_cache(false)`: BigQuery answers from its query cache by default;
- `.timeout(..)`: how long the first `Query` call waits for the job, 10 seconds by default. A job
  still running then is polled until it completes;
- `.job_timeout(..)`: how long BigQuery lets the job run before it cancels the job itself;
- `.request_id(..)`: the idempotency key of the `Query` call, see [DML](#dml-and-ddl);
- `.inline_rows_limit(..)` and `.read_options(..)`: how the rows come back, see
  [Where the rows come from](#where-the-rows-come-from).
- `.destination_table(..)`: a table of your own for the result, see
  [Destination tables](#destination-tables).

```rust,no_run
use bigquery::*;
use serde::Deserialize;
use std::time::Duration;

const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");

#[derive(Debug, Deserialize)]
struct Order {
    id: i64,
}

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let orders: Vec<Order> = db
    .fluent()
    .query("SELECT id FROM orders WHERE status = 'new'")
    .default_dataset(SHOP)
    .labels([("team", "shop"), ("report", "new-orders")])
    .maximum_bytes_billed(1_000_000_000)
    .job_timeout(Duration::from_secs(60))
    .obj::<Order>()
    .query()
    .await?;
# let _ = orders;
# Ok(())
# }
```

## Short query mode

By default the library sends every query with BigQuery's short query mode
(`JOB_CREATION_OPTIONAL`), as Google's own Rust crate does. BigQuery then answers a short query
whose result fits in the first response without creating a job. In the
[benchmarks](./benchmarks.md#small-query-latency) creating the job cost 80-165 ms.

A query that ran without a job:

- reports a `query_id` and no `job` in its stats and outcome;
- is still listed in the `INFORMATION_SCHEMA.JOBS` views, with its `query_id` as the `job_id` and
  with its labels;
- has nothing for `get_job` to read or `cancel_job` to cancel. `GetJob` refuses its `query_id`,
  since it is not a job ID.

BigQuery still creates a job for a query that runs long, a result too large for the response, and
DML and DDL statements (every one of them got a job in the library's live tests).

`.job_creation_required()` makes every query create a job. Use it when the query needs a job
resource: for job history read through the job calls, for a job ID it is sure to get, or to cancel
it by its job.

```rust,no_run
use bigquery::*;

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let outcome = db
    .fluent()
    .query("SELECT 1")
    .job_creation_required()
    .execute()
    .await?;

let job = outcome.job.expect("a required job is always reported");
println!("{} in {:?}", job.job_id, job.location);
# Ok(())
# }
```

A retry of a failed `Query` call always requires a job, whatever the first attempt asked for.
BigQuery recognises a repeated `request_id` and returns the job of the first attempt only in that
mode, and answers it with `AlreadyExists` in the optional one.

## DML and DDL

`execute()` runs a statement, waits for it, and returns a `BigQueryQueryOutcome` without reading
any rows:

```rust,no_run
use bigquery::*;

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let outcome = db
    .fluent()
    .query("UPDATE shop.orders SET status = 'shipped' WHERE id = @id")
    .param("id", 42)
    .execute()
    .await?;

println!(
    "{:?} changed {:?} rows: {:?}",
    outcome.statement_type, outcome.num_dml_affected_rows, outcome.dml_stats
);
# Ok(())
# }
```

`BigQueryQueryOutcome` is the same `BigQueryJobStats` the queries return, see
[Job stats](#job-stats). For DML it has:

- `statement_type`: `Some(BigQueryStatementType::Update)`, `Insert`, `Delete`, `Merge`, etc.;
- `num_dml_affected_rows`: the rows the statement changed;
- `dml_stats`: the rows inserted, updated and deleted, as `BigQueryDmlStats`.

DDL (`CREATE TABLE`, `ALTER TABLE`, etc.) goes through `execute()` the same way. `.obj::<T>()` on
a DML or DDL statement is just an empty result.

Every terminal call sends a fresh random `request_id` unless you set one, and every retry of that
call repeats it, so a retried DML statement is not run twice. BigQuery keeps the keys for a limited
time window. Set your own `BigQueryRequestId` with `.request_id(..)` if your code may send the same
statement again on its own, for example after a restart:

```rust,no_run
use bigquery::*;

# async fn example(db: BigQueryDb, order_id: i64) -> BigQueryResult<()> {
let request_id = BigQueryRequestId::new(format!("ship-order-{order_id}"))?;
let outcome = db
    .fluent()
    .query("UPDATE shop.orders SET status = 'shipped' WHERE id = @id")
    .param("id", order_id)
    .request_id(request_id)
    .execute()
    .await?;
# let _ = outcome;
# Ok(())
# }
```

A statement that fails is an error from the terminal: a syntax error, a missing table or
`ERROR()` in the SQL come back from the `Query` call itself, and a job that finished with an error
is a `JobError` with BigQuery's reason and messages.

## Dry run

`dry_run()` validates the statement and returns the bytes it would process and the schema of its
result, without running it. It never creates a job and bills nothing.

With a [destination table](#destination-tables) the dry run is sent as a dry-run job with the
same destination, so BigQuery checks that part too. It still creates no job and writes nothing.

```rust,no_run
use bigquery::*;

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let estimate = db
    .fluent()
    .query("SELECT * FROM `bigquery-public-data.samples.shakespeare`")
    .dry_run()
    .await?;

println!("Would process {:?} bytes", estimate.total_bytes_processed);
if let Some(schema) = estimate.schema {
    for field in schema.fields {
        println!("{}: {}", field.name, field.field_type);
    }
}
# Ok(())
# }
```

Full example available [here](https://github.com/abdolence/bigquery-rs/blob/master/examples/dml-and-dry-run.rs).

## Job stats

`query_with_stats()` returns the rows together with what the query used, as `BigQueryJobStats`:

- `job`: the job that ran the query, `None` when BigQuery ran it without one;
- `query_id`: the ID BigQuery gave the query, with or without a job;
- `statement_type`: the kind of statement;
- `total_rows`: rows in the result;
- `total_bytes_processed` and `total_bytes_billed`: bytes processed, and bytes billed after
  BigQuery's rounding and minimums;
- `total_slot_ms`: slot milliseconds the job used;
- `cache_hit`: whether the query cache answered;
- `num_dml_affected_rows` and `dml_stats`: for DML.

```rust,no_run
use bigquery::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Total {
    words: i64,
}

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let (rows, stats) = db
    .fluent()
    .query("SELECT SUM(word_count) AS words FROM `bigquery-public-data.samples.shakespeare`")
    .obj::<Total>()
    .query_with_stats()
    .await?;

println!(
    "{rows:?}: {:?} bytes billed, {:?} slot ms, cache hit {:?}",
    stats.total_bytes_billed, stats.total_slot_ms, stats.cache_hit
);
# Ok(())
# }
```

A figure BigQuery did not report is `None`, never `0`. The library makes no extra call for the
stats: every figure comes from a response the query receives anyway. A result answered in the
first `Query` response has only that response's figures. A larger one also has the figures of the
job's statistics, which the query reads to find its destination table.

`stream_query_with_stats()` returns the stats together with the stream, before any row. The query
waits for its job to finish before the first row streams, so every job figure is already known
then. What reading the rows cost is on the read's span, see [Observability](./observability.md).

Even a query that reads no table uses slots. On 1,000 generated rows BigQuery reported 0 bytes
processed and billed, and 25 slot ms answered inline, 152 slot ms read through Storage Read.

Full example available [here](https://github.com/abdolence/bigquery-rs/blob/master/examples/job-stats-tracing.rs).

## Cancellation

Dropping a query's stream or future does not cancel its job, BigQuery runs it to the end. To limit
how long a job can run, set `.job_timeout(..)`, and BigQuery cancels it itself.

To cancel a job from your code use `cancel_job` with its `BigQueryJobRef`. It returns once BigQuery
accepted the request, which is before the job stops, and a job that already finished stays
finished.

A query terminal returns only when its job has finished, so to cancel a long query find it while it
runs, for example by a label:

```rust,no_run
use bigquery::*;
use futures::StreamExt;

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
// The long query was started elsewhere with .label("report", "monthly-totals")
let mut running = db
    .stream_jobs(BigQueryListJobsParams::new().with_states(vec![BigQueryJobState::Running]))
    .await?;

while let Some(job) = running.next().await {
    if job.labels.get("report") == Some("monthly-totals") {
        db.cancel_job(&job.reference).await?;
    }
}
# Ok(())
# }
```

A query that runs long always has a job, even in short query mode, since BigQuery creates one for
a query that outlives the first `Query` call.

## Where the rows come from

Every query asks BigQuery for an Arrow result. Then:

- a result that comes complete in the first `Query` response is decoded from the inline Arrow
  in that response;
- a larger result, or one whose job outlived the first call, is read from the job's destination
  table through the Storage Read API, with several streams in parallel;
- a query with a [destination table](#destination-tables) is always read from that table
  through the Storage Read API;
- a statement without rows, such as DML or DDL, reads nothing.

Both paths use the same Arrow decoder as table reads, so a type maps the same way in a query and
in a table read. The query's span records which path it took, as `/bigquery/route`.

BigQuery decides how much of a result goes inline. A small result reaches its last row sooner
inline, since a Storage Read session costs a call to open before the first row. A large result is
much faster through Storage Read: in the benchmarks 200,000 rows took 1.16 s, against 4.39 s for
the official crate and 4.55 s for Python reading the same result as REST pages.

`.inline_rows_limit(rows)` caps the rows of the first response, so a result with more goes to
Storage Read. `.read_options(..)` sets how that read opens its session, the same
`BigQueryReadOptions` as for table reads:

```rust,no_run
use bigquery::*;

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let batches = db
    .fluent()
    .query("SELECT * FROM `bigquery-public-data.samples.shakespeare`")
    .inline_rows_limit(10_000)
    .read_options(
        BigQueryReadOptions::new()
            .with_max_stream_count(4)
            .with_compression(BigQueryReadCompression::Zstd),
    )
    .record_batches()
    .await?;
# let _ = batches;
# Ok(())
# }
```

By default the read asks for as many streams as the machine has parallelism, with LZ4 compression,
and BigQuery decides how many it actually gives.

## Destination tables

BigQuery writes every query result into a temporary table, and you can name a table of your own
instead. The table is a setting of the job, so the query stays a plain `SELECT`:

- `.destination_table(table)`: the job writes the result into `table`, and fails if the table
  already holds rows. This is BigQuery's own default, `WRITE_EMPTY`, so nothing is overwritten
  unless you ask for it;
- `.append_to_destination_table(table)`: the result is added after the rows the table holds
  (`WRITE_APPEND`);
- `.dangerously_overwrite_destination_table(table)`: the result replaces every row of the table
  and its schema (`WRITE_TRUNCATE`).

BigQuery creates the table when it does not exist, with the schema of the result. Each write
applies only when the job succeeds, as one update of the table.

```rust,no_run
use bigquery::*;
use serde::Deserialize;

const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
const CITY_TOTALS: BigQueryTableId = BigQueryTableId::from_static("city_totals");

#[derive(Debug, Deserialize)]
struct CityTotal {
    city: String,
    total: f64,
}

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let totals: Vec<CityTotal> = db
    .fluent()
    .query(
        "SELECT c.city, SUM(o.total) AS total FROM shop.orders o \
         JOIN shop.customers c ON c.id = o.customer_id GROUP BY c.city",
    )
    .destination_table(SHOP.table(CITY_TOTALS))
    .obj::<CityTotal>()
    .query()
    .await?;
# let _ = totals;
# Ok(())
# }
```

Every terminal works with a destination. The rows are read back from the table through the
Storage Read API, so after an append they are the whole table's, the rows it held before
included. `execute()` writes the table and reads nothing back.

Be aware a table keeps no row order, so the rows come back in no particular order, even with
`ORDER BY` in the query and even for a small result, which never comes inline here. Sort them on
your side if the order matters.

A write into a table that already holds rows, with `.destination_table(..)`, fails with a
`DataConflictError`, BigQuery's `AlreadyExists` for that table.

The query always runs as a job, inserted with `InsertJob`, since the `Query` call has no
destination field. So `.job_creation_required()`, `.inline_rows_limit(..)` and `.request_id(..)`
do not apply: the job's own ID, new for each terminal call, makes a retried `InsertJob` safe. The
query cache does not answer a query with a destination table either.

Be aware the table is not temporary: it stays until you delete it or it expires, and its storage
and the Storage Read of it are billed, see
[Billing hints](./table-reads-or-queries.md#billing-hints).

Full example available [here](https://github.com/abdolence/bigquery-rs/blob/master/examples/query-destination-table.rs).
