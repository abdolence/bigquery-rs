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

To start using the library, see [Getting started](./getting-started.md).

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
  [benchmarks](./benchmarks.md):
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
- It has a documented client for every v2 service, including models, routines, row access
  policies and projects, and every field of the query request (sessions, external tables,
  encryption, slot limits, etc.). This crate has its own API for datasets, tables, jobs and the
  common query settings, and for the rest only the raw gRPC clients from gcloud-sdk, such as
  `model_client()` and `routine_client()`;
- Its writer takes Arrow record batches and supports buffered streams. This crate writes your
  structures as protobuf, through the default, committed and pending streams;
- It has stub traits to mock its clients in your tests. This crate has no public mocks.

## Crypto provider error

Depends on your other dependencies you may see the error like:

```text
no process-level CryptoProvider available -- call CryptoProvider::install_default() before this point
```

The TLS crypto providers are not installed by default, so you can choose one. The easiest way to
fix it is to include one, for example:

```toml
[dependencies]
rustls = "0.23"
```

If you have several, you may need to call `CryptoProvider::install_default()` before creating the
client:

```rust,ignore
rustls::crypto::ring::default_provider().install_default().expect("Failed to install rustls crypto provider");
```
