# Benchmarks

The library was compared with Google's own clients on the same machine, region and data:

- the official Rust crate [google-cloud-bigquery](https://crates.io/crates/google-cloud-bigquery);
- Python `google-cloud-bigquery` with `google-cloud-bigquery-storage` and `pyarrow`;
- the `bq` CLI, for query latency only.

The numbers are from 2026-10-04, both on 0.1.0: the full run of every scenario, and a second
run of the query scenarios, the query run. They depend a lot on the network, so treat them as a
comparison between the clients on one connection, not as absolute figures.

## Method

- **Machine:** Intel Core i7-10700K, 16 threads, 64 GB RAM, Linux 7.2, CPU governor
  `powersave`. A home connection in Sweden.
- **Region:** every table in `europe-north2` (Stockholm), and every query sent with that
  location, so generated rows are computed there too.
- **Endpoints:** the global defaults for every client, `bigquery.googleapis.com` and
  `bigquerystorage.googleapis.com`. The TCP connect to both took about 10 ms and the TLS
  handshake about 29 ms. A small authenticated call, `datasets.get` over the library's warm gRPC
  channel, took about 0.1 s.
- **Data:** generated once with `CREATE TABLE ... AS SELECT` over `GENERATE_ARRAY`, in a
  scratch dataset deleted at the end. The scan table has 1M rows and 20 columns: INT64,
  FLOAT64, STRING, BOOL, NUMERIC, DATE and TIMESTAMP, a STRUCT and an ARRAY, some of them
  nullable. It is about 215 MB as BigQuery counts it.
- **Runs:** 1 warm-up and 5 measured runs for every client and scenario, timed inside the
  client process (outside of it for `bq`). The tables show the median and the range.
- **Order:** one client at a time. The clients run round by round, run 1 of every client, then
  run 2, etc., with the order rotated each round, so drifts in the machine or the network spread
  over all of them.
- **Quiet machine:** before each run the harness waited for a 1-minute load under 1.6 (10% of
  the cores), no other process above 20% of a core, no compiler or linker running and under
  2 MB/s of idle network traffic. During the run it sampled the machine every 2 s and repeated a
  run with a bad sample. In the full run 4 runs were repeated, all because of a busy editor
  on the same desktop; in the query run 4 more, 3 of them because of a Rust build on the same
  machine. None were left contended. Every reported run had other processes using under
  0.6 cores, and the network traffic during the runs matched what the clients themselves moved.
- **Settings:** the defaults of each client, query cache off everywhere. Where a client leaves a
  setting to the caller, the harness set it:
  - Storage Read on the official crate: 16 streams asked for (the library asks for the machine's
    parallelism), LZ4 buffers, one task per stream;
  - Storage Write on the official crate: 25,000 rows per Arrow request, about 5.5 MB, so a request
    stays under the 10 MB limit, and 8 requests in flight, which is the library's default window.

Python asks Storage Read for `max_stream_count = 0` by default, which lets BigQuery decide. It got
1 stream every time, while on the 1M-row table scan the library and the official crate got 4 of the
16 they asked for.

## Versions

| Client | Versions |
|---|---|
| bigquery (this library) | 0.1.0; gcloud-sdk 0.32.4 (0.32.3 for the full run), tonic 0.14.6, arrow 60.0.0 |
| google-cloud-bigquery | 0.18.0, google-cloud-bigquery-v2 1.0.0, google-cloud-gax 1.15.0, google-cloud-auth 1.17.0 |
| Python | Python 3.14.7, google-cloud-bigquery 3.46.1, google-cloud-bigquery-storage 2.42.0, pyarrow 25.0.1, pandas 3.0.6, grpcio 1.84.0 |
| bq | BigQuery CLI 2.1.39 (Google Cloud SDK 587.0.0) |

Rust code was built in release mode with rustc 1.98.1.

## What each client does

The paths below were checked in each client's source for the versions above, and the harness
records which one each run actually took:

- **This library:** everything over gRPC. A query goes through the v2 `Query` call with an
  Arrow result and `JOB_CREATION_OPTIONAL` by default (`.job_creation_required()` turns it off);
  a result that does not fit in the first response is read through Storage Read.
  Typed rows are decoded from Arrow with serde.
- **google-cloud-bigquery:** queries over REST (`google-cloud-bigquery-v2`), rows as JSON pages
  converted with `#[derive(FromRow)]`. It sends queries with `JOB_CREATION_OPTIONAL` by default,
  so a short query may run without a job. Storage Read is a raw generated client that returns
  Arrow IPC bytes and leaves the decoding to you; in 0.18 it is not behind any `cfg` flag. Storage
  Write takes Arrow you serialize yourself; its protobuf writer is not public.
- **Python:** `query_and_wait()` over REST, and `to_arrow()` or `to_dataframe()` through Storage
  Read for large results and table scans.
- **bq:** one CLI process per query.

## Small query latency

The library is measured twice, in its default short query mode and with
`.job_creation_required()`, so you can see what a job costs. Both run in the same harness run
as the other clients.

`SELECT 1 AS x`:

| Client | Median | Range |
|---|---|---|
| bigquery | 0.096 s | 0.089-0.115 s |
| bigquery, `.job_creation_required()` | 0.167 s | 0.160-0.256 s |
| google-cloud-bigquery | 0.096 s | 0.086-0.110 s |
| Python | 0.173 s | 0.144-0.272 s |
| bq | 2.354 s | 2.325-2.476 s |

1,000 generated rows:

| Client | Median | Range | Rows/s |
|---|---|---|---|
| bigquery | 0.130 s | 0.110-0.145 s | 7,676 |
| bigquery, `.job_creation_required()` | 0.203 s | 0.177-0.207 s | 4,930 |
| google-cloud-bigquery | 0.134 s | 0.117-0.137 s | 7,480 |
| Python | 0.213 s | 0.195-0.284 s | 4,690 |
| bq | 2.392 s | 2.338-2.402 s | 418 |

The library and the official crate are the same speed here, and both are faster than Python,
because of the job:

- The official crate sets `JobCreationMode::JobCreationOptional` on every query
  (`google-cloud-bigquery` 0.18.0, `src/query/builder.rs`, line 73), BigQuery's short query
  mode, where BigQuery may answer without creating a job. The library does the same by default.
  In these runs neither got a job back, while Python and the library's required mode got one
  every time.
- Measured in one process on the library's own channel, the library adds nothing over a raw
  `Query` call: 0.086 s against 0.085-0.092 s for the constant query in short query mode, and
  0.151 s against 0.149 s with a job. Creating the job is the whole difference, about 65 ms.

About 0.6 s of every `bq` run is the CLI starting up (`bq version` timed in each run); the rest
probably is its job insert and polling, I didn't check.

## Large query result

200,000 generated rows of 4 columns, read as typed rows, from the query run:

| Client | Path | Median | Range | Rows/s |
|---|---|---|---|---|
| bigquery | Storage Read, 1 stream | 1.293 s | 1.246-1.484 s | 154,674 |
| bigquery, `.job_creation_required()` | Storage Read, 1 stream | 1.279 s | 1.100-1.564 s | 156,343 |
| google-cloud-bigquery | REST JSON pages | 3.862 s | 3.617-4.088 s | 51,787 |
| Python (`list(rows)`) | REST JSON pages | 4.704 s | 4.567-5.004 s | 42,515 |

A result this large always gets a job, so the two modes are the same here. The full run
gave 1.260 s, 3.690 s and 4.935 s.

The same result as Arrow, from the full run:

| Client | Path | Median | Range | Rows/s |
|---|---|---|---|---|
| bigquery (`record_batches()`) | Storage Read, 1 stream | 1.124 s | 1.107-1.202 s | 177,868 |
| Python (`to_arrow()`) | Storage Read, 1 stream | 2.373 s | 2.351-2.608 s | 84,289 |
| google-cloud-bigquery | n/a: its query client returns JSON rows only | | | |

The library reads the destination table through Storage Read once the result does not fit in
the first response, while the official crate and Python iterate REST pages. The official crate
spent 0.019 s of its 3.7-3.9 s in `FromRow`, so nearly all of its time is the paging itself.

Python takes the same Storage Read path for `to_arrow()` and is still twice as slow. I guess it
is the extra metadata calls before the read session, but I didn't measure that.

## Table scan

The whole 1M-row table, every column. MB/s is the table's 215 MB divided by the median.

Typed rows:

| Client | Path | Median | Range | Rows/s | MB/s |
|---|---|---|---|---|---|
| bigquery (`obj::<T>()`) | Storage Read, 4 streams | 9.764 s | 7.708-10.307 s | 102,414 | 22.0 |
| Python (`to_dataframe()`) | Storage Read, 1 stream | 10.746 s | 10.494-11.396 s | 93,061 | 20.0 |
| google-cloud-bigquery (`FromRow`) | `SELECT *` query, REST JSON pages | 94.723 s | 92.798-94.884 s | 10,557 | 2.3 |

Arrow:

| Client | Path | Median | Range | Rows/s | MB/s |
|---|---|---|---|---|---|
| bigquery (`record_batches()`) | Storage Read, 4 streams | 9.344 s | 7.516-9.722 s | 107,018 | 23.0 |
| google-cloud-bigquery (raw client + `arrow-ipc`) | Storage Read, 4 streams | 9.276 s | 7.336-9.713 s | 107,801 | 23.1 |
| Python (`to_arrow()`) | Storage Read, 1 stream | 9.918 s | 9.767-10.579 s | 100,830 | 21.6 |

The official crate has no typed Storage Read, so its typed path is a query, and a `SELECT *`
query bills the table: 215 MB per run. Its `FromRow` took about 1.05 s of the 95 s.

Raw Arrow scans are the same speed in all three clients, and the library has no advantage there.
Every client moved about 150 MB per scan on the network interface at about 16 MB/s, so I think
this connection is the limit, not the clients.

Python's `to_dataframe()` builds a pandas frame with typed columns, not row objects, so it is the
closest Python has to typed rows rather than the same thing.

## Storage Write

1M rows of the scan table's shape into the default stream, per run:

| Client | Format | Median | Range | Rows/s | MB sent |
|---|---|---|---|---|---|
| bigquery (`insert().objects()`) | protobuf from serde | 29.957 s | 25.186-30.267 s | 33,381 | 180.5 |
| google-cloud-bigquery | Arrow built from the same rows | 32.715 s | 30.786-33.363 s | 30,567 | 221.4 |
| Python | n/a: its writer takes requests you build yourself, protobuf rows with a hand-made descriptor | | | | |

Both times include turning the Rust rows into the wire format. The library sent 18% fewer bytes.
On the network interface the library ran at about 6.5 MB/s and the official crate at about
7.3 MB/s, so the upload of this connection is probably not the whole limit. I think the smaller
requests are why the library is a bit faster, but that is not measured.

## Decode cost

The scan table's 1M rows already in memory as Arrow batches, decoded into structs on one thread:

| Client | Median | Range | Rows/s |
|---|---|---|---|
| bigquery | 0.649 s | 0.642-0.651 s | 1,541,720 |
| google-cloud-bigquery | n/a | | |

The official crate has no Arrow to struct decoder. Its `FromRow` converts the JSON rows of a live
query, and `Row` has no public constructor, so it cannot be fed the same batches. Inside the REST
scan above its conversion took about 1.05 s per 1M rows, but on already parsed JSON values, so the
two numbers are not comparable.

In the library the decode runs on each read stream's task, next to the network reads, which is
probably why typed and Arrow scans take almost the same time.

## Cost

The full run billed 1.72 GB of queries, all of them the official crate's `SELECT *` scans, and the
query run billed 0 bytes. Every other query read generated rows and billed 0 bytes. The scans
went through the Storage Read free tier, and the writes ingested about 215 MB per 1M rows under the Storage Write free tier.

## Reproducing

The harness is in [bench-compare](https://github.com/abdolence/bigquery-rs/tree/main/bench-compare),
an unpublished crate with the Rust contenders and a [uv](https://docs.astral.sh/uv/) project for
Python. You need application default credentials and optionally the `bq` CLI:

```sh
bench-compare/run.sh --project my-project
```

It creates the scratch dataset in `europe-north2` (`--location` changes it), runs everything and
deletes the dataset, also on failure. The raw results land in
`bench-compare/results/<run label>/results.json`, with the machine state of every run. A full run
takes about 35 minutes and costs a few cents. `--only query_const,query_1k,query_200k_rows` runs
just the query scenarios. The summary of both runs above is in
`bench-compare/results-2026-10-04-europe-north2.json`.
