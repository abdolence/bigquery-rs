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
    - Query results written into a destination table of your own and read back the same way;
    - Raw Arrow record batches for table reads and query results;
    - DML statements with affected row counts, and dry runs;
    - Writes through the Storage Write API with built-in batching and backpressure, of your
      structures or of raw Arrow record batches: at least once, exactly once, atomic (all rows
      or none), or buffered (rows readable once you flush them);
    - Updates and deletes by the primary key through change data capture (CDC), with fluent
      `update()` and `delete()` or a lower-level CDC writer;
    - Declarative table schemas, inferred from your structures or declared by hand, planned and
      synced with one call: new columns, renames, drops, widening, partitioning, clustering,
      primary key, recreating an empty table;
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

- **High-level typed API.** Fluent builders for table reads, queries, writes, updates and
  deletes, and table schemas. Rows are your own structures in both directions, through Serde,
  for table reads, query results, writes and CDC. Dataset and table
  IDs are checked types, column paths come from your structure fields with `path!`/`paths!`,
  and table schemas are declared once and planned or synced with `.plan()`/`.sync()`.
- **gRPC throughout.** Queries, jobs, datasets and tables go through the BigQuery v2 API over
  gRPC, as well as Storage Read and Write. The official crate sends queries over REST and reads
  their rows as JSON pages.
- **Performance.** Measured on 2026-10-05 against the official crate and Python on one home
  connection in Sweden to `europe-north2`, full details in the
  [benchmarks](./benchmarks.md):
    - small queries take 0.117 s for `SELECT 1`, about the same as the official crate, which
      also uses BigQuery's short query mode, and faster than Python's 0.270 s, since Python
      creates a job;
    - a 200,000-row query result as typed rows takes 1.16 s against 4.39 s, because the
      library reads a large result through Storage Read and the official crate pages it as
      JSON;
    - a 1M-row table scan as typed rows takes 9.6 s. The official crate has no typed Storage
      Read, so its typed path is a `SELECT *` query, which took 95.2 s and billed the whole
      table every run;
    - a 1M-row Storage Write from structures takes 34.0 s against 35.4 s, with 18% fewer bytes
      sent as protobuf than the official crate's Arrow.
- **Observability.** Every query, read and write span carries what BigQuery reports: bytes
  processed and billed, slot milliseconds, cache hits, rows and bytes read, rows appended and
  bytes sent, retries. `query_with_stats()` returns a query's figures together with its rows.
- **Safety.** Query parameter values are always bound on the server side and never written into
  the SQL text. The typed filter and the generated DDL write values as escaped literals and names
  as quoted identifiers, tested against a corpus of hostile values and on BigQuery itself. The
  calls that lose data say so in their names: `dangerously_delete_with_contents()`,
  `dangerously_recreate_with_data_loss()`. No unsafe code.
- **Testing.** A fake BigQuery for your tests, behind the `testing` feature. The code under test
  runs its real client against a loopback gRPC server, which answers queries, serves table rows
  and fails calls as the test scripts it with your own structures. No credentials or network.

### What the official crate provides and this one does not

Checked against `google-cloud-bigquery` 0.18.0 and `google-cloud-bigquery-v2` 1.0.0:

- It is maintained by Google as part of
  [google-cloud-rust](https://github.com/googleapis/google-cloud-rust), and its v2 API crate is
  already 1.0;
- It uses the REST API for v2, which is GA. The v2 API over gRPC this crate uses works for every
  call the library makes, but Google does not document it and it is pre-GA, so be aware it can
  change without notice;
- It has stub traits to mock its clients in your tests. This crate has no mocks of its clients
  and provides a fake BigQuery server instead, see [Testing with the fake](./testing.md).
