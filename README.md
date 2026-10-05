[![Cargo](https://img.shields.io/crates/v/bigquery.svg)](https://crates.io/crates/bigquery)
![tests and formatting](https://github.com/abdolence/bigquery-rs/workflows/tests%20&%20formatting/badge.svg)
![security audit](https://github.com/abdolence/bigquery-rs/workflows/security%20audit/badge.svg)

# BigQuery for Rust

Library provides a simple API for Google BigQuery using gRPC for every call:

- Fluent high-level and strongly typed API;
- Read and write rows as Rust structures using Serde, with own codecs for every BigQuery type,
  including NUMERIC/BIGNUMERIC, INTERVAL, RANGE, nested STRUCT and ARRAY, and JSON columns
  mapped by the field type (`String`, `serde_json::Value` or your own structure);
- Support for:
    - Table reads through the Storage Read API, in parallel streams, with column projection
      and typed filters;
    - Queries with named and positional parameters, including ARRAY and STRUCT parameters,
      always bound on the server side;
    - Short query mode, so BigQuery can answer short queries without creating a job;
    - Large query results read through the Storage Read API automatically;
    - Raw Arrow record batches for table reads and query results;
    - DML statements with affected row counts, and dry runs;
    - Writes through the Storage Write API with built-in batching and backpressure: at least
      once, exactly once, or atomic (all rows or none);
    - Change data capture (CDC): upserts and deletes by the primary key;
    - Declarative table schemas, planned and synced with one call: new columns, renames, drops,
      widening, partitioning, clustering, primary key, recreating an empty table;
    - Datasets, tables and jobs management;
    - Bytes processed and billed, slot milliseconds and cache hits for every query, as span
      fields and as results;
- Retries with jitter for the errors where retrying makes sense, including BigQuery rate limits;
- Full async based on Tokio runtime;
- Macros that help you use your structure fields as column paths;
- Google client based on [gcloud-sdk library](https://github.com/abdolence/gcloud-sdk-rs)
  that automatically detects GCE environment or application default accounts for local development;

## Documentation

Please follow to the official website: <https://bigquery-rust.abdolence.dev>.

## Quick start

Cargo.toml:

```toml
[dependencies]
bigquery = "0.5"
```

```rust,no_run
use bigquery::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Order {
    id: i64,
    customer: String,
    total: f64,
    placed_at: jiff::Timestamp,
}

#[derive(Debug, Deserialize)]
struct CustomerTotal {
    customer: String,
    total: f64,
}

const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = BigQueryDb::new("my-gcp-project-id").await?;

    // Create a dataset
    db.fluent()
        .schema()
        .dataset(SHOP)
        .create()
        .location(BigQueryLocation::from_static("EU"))
        .execute()
        .await?;

    // Create the table, or bring an existing one to this schema
    let report = db
        .fluent()
        .schema()
        .table(SHOP.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field(path!(Order::id)).int64().required(),
                columns.field(path!(Order::customer)).string(),
                columns.field(path!(Order::total)).float64(),
                columns.field(path!(Order::placed_at)).timestamp(),
            ])
        })
        .primary_key([path!(Order::id)])
        .partition_by_day(path!(Order::placed_at))
        .sync()
        .await?;
    println!("{report}");

    // Insert through the Storage Write API
    let orders = vec![
        Order {
            id: 1,
            customer: "Alice".to_string(),
            total: 120.0,
            placed_at: jiff::Timestamp::now(),
        },
        Order {
            id: 2,
            customer: "Bob".to_string(),
            total: 15.5,
            placed_at: jiff::Timestamp::now(),
        },
    ];
    let summary = db
        .fluent()
        .insert()
        .into(SHOP.table(ORDERS))
        .objects(&orders)
        .execute()
        .await?;
    println!("Written {} rows", summary.rows_written);

    // Read the table through the Storage Read API, with a typed filter
    let large_orders: Vec<Order> = db
        .fluent()
        .select()
        .from(SHOP.table(ORDERS))
        .filter(|filter| {
            filter.for_all([
                filter.field(path!(Order::customer)).eq("Alice"),
                filter.field(path!(Order::total)).gt(100.0),
            ])
        })
        .obj()
        .query()
        .await?;

    // Query with parameters, and what the query cost
    let (totals, stats): (Vec<CustomerTotal>, BigQueryJobStats) = db
        .fluent()
        .query(
            "SELECT customer, SUM(total) AS total FROM shop.orders \
             WHERE customer = @customer GROUP BY customer",
        )
        .param("customer", "Alice")
        .obj()
        .query_with_stats()
        .await?;

    println!(
        "{large_orders:?} {totals:?}, billed {:?} bytes",
        stats.total_bytes_billed
    );

    Ok(())
}
```

If you see `no process-level CryptoProvider available`, see
[Crypto provider error](https://bigquery-rust.abdolence.dev/intro.html#crypto-provider-error).

## Examples

All examples available in the [examples](examples) directory.

To run an example with environment variables:

```bash
PROJECT_ID=<your-google-project-id> cargo run --example insert
```

## Why this crate

Google has its own Rust crate for BigQuery,
[google-cloud-bigquery](https://crates.io/crates/google-cloud-bigquery) (0.18.0 at the time of
writing, October 2026). Both crates work with the same APIs in different ways, so below is what
each of them provides that the other does not.

### What this crate provides

- **High-level typed API.** Fluent builders in the same style as
  [firestore-rs](https://github.com/abdolence/firestore-rs). Rows are your own structures in both
  directions, through Serde, for table reads, query results, writes and CDC. Dataset and table
  IDs are checked types, column paths come from your structure fields with `path!`/`paths!`,
  and table schemas are declared once and planned or synced with `.plan()`/`.sync()`.
- **gRPC throughout.** Queries, jobs, datasets and tables go through the BigQuery v2 API over
  gRPC, as well as Storage Read and Write. The official crate sends queries over REST and reads
  their rows as JSON pages.
- **Performance.** Measured on 2026-10-04 against the official crate and Python on one home
  connection in Sweden to `europe-north2`, full details in the
  [benchmarks](https://bigquery-rust.abdolence.dev/benchmarks.html):
    - small queries take 0.096 s for `SELECT 1`, the same as the official crate, which also uses
      BigQuery's short query mode, and faster than Python's 0.173 s, since Python creates a job;
    - a 200,000-row query result as typed rows takes 1.29 s against 3.86 s, because the
      library reads a large result through Storage Read and the official crate pages it as
      JSON;
    - a 1M-row table scan as typed rows takes 9.8 s. The official crate has no typed Storage
      Read, so its typed path is a `SELECT *` query, which took 94.7 s and billed the whole
      table every run;
    - a 1M-row Storage Write takes 30.0 s against 32.7 s, with 18% fewer bytes sent as
      protobuf than the official crate's Arrow. I think the smaller requests are why it is a bit
      faster.
- **Observability.** Every query, read and write span carries what BigQuery reports: bytes
  processed and billed, slot milliseconds, cache hits, rows and bytes read, rows appended and
  bytes sent, retries. `query_with_stats()` returns a query's figures together with its rows.
- **Safety.** Query parameter values are always bound on the server side and never written into
  the SQL text. The typed filter and the generated DDL write values as escaped literals and names
  as quoted identifiers, tested against a corpus of hostile values and on BigQuery itself. The
  calls that lose data say so in their names: `dangerously_delete_with_contents()`,
  `dangerously_recreate_with_data_loss()`. No unsafe code.

### What the official crate provides and this one does not

Checked against `google-cloud-bigquery` 0.18.0 and `google-cloud-bigquery-v2` 1.0.0:

- It is maintained by Google as part of
  [google-cloud-rust](https://github.com/googleapis/google-cloud-rust), and its v2 API crate is
  already 1.0;
- It uses the REST API for v2, which is GA. The v2 API over gRPC this crate uses works for every
  call the library makes, but Google does not document it and it is pre-GA, so be aware it can
  change without notice;
- Its writer takes Arrow record batches and supports buffered streams. This crate writes your
  structures as protobuf, through the default, committed and pending streams;
- It has stub traits to mock its clients in your tests. This crate has no public mocks.

## Google authentication

Looks for credentials in the following places, preferring the first location found:

- A JSON file whose path is specified by the GOOGLE_APPLICATION_CREDENTIALS environment variable.
- A JSON file in a location known to the gcloud command-line tool using `gcloud auth application-default login`.
- On Google Compute Engine, it fetches credentials from the metadata server.

For local development don't confuse `gcloud auth login` with `gcloud auth application-default login`,
since the first authorize only `gcloud` tool to access the Cloud Platform.

## How this library is tested

There are unit tests next to the code, many of them against a fake BigQuery gRPC server, and the
code in the book is compiled as doctests. The integration tests in the tests directory run for
every push to master against a real BigQuery project allocated for testing purposes, or locally when
`GCP_PROJECT` is set. Other branches run only the unit tests and doctests.
All of them work in one dataset, `bigquery_rs_ci`, under an account whose BigQuery access is that
dataset and nothing else: it can run jobs and read sessions, but it cannot create a dataset or
read or write data outside this one. Each test makes its own tables there, named after its run,
and drops them at the end; the dataset expires anything left behind after an hour. Be aware not
to introduce huge reads, writes or queries there, since they are billed.

## Licence

Apache Software License (ASL)

## Author

Abdulla Abdurakhmanov
