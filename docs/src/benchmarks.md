# Benchmarks

The library was compared with Google's own clients on the same machine, region and data:

- the official Rust crate [google-cloud-bigquery](https://crates.io/crates/google-cloud-bigquery);
- Python `google-cloud-bigquery` with `google-cloud-bigquery-storage` and `pyarrow`;
- the `bq` CLI, for query latency only.

The numbers are from 2026-10-05, on `master` after 0.5.0, commit `5f30c65`: the full run of
every scenario, a second run of the query scenarios with `bq`, the query run, and a rerun of the
Arrow write. They depend a lot on the network, so treat them as a comparison between the clients
on one connection, not as absolute figures.

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
  run with a bad sample. In the full run 12 attempts were repeated, all because of the browser, the
  desktop compositor or a snapshot daemon on the same machine. One Arrow write run stayed
  contended after 4 attempts, so that scenario was run again on its own, with no run repeated.
  The query run repeated none. Every reported run started at a 1-minute load under 0.9 and had
  other processes using under 0.9 cores, and the network traffic during the runs matched what the
  clients themselves moved.
- **Settings:** the defaults of each client, query cache off everywhere. Where a client leaves a
  setting to the caller, the harness set it:
  - Storage Read on the official crate: 16 streams asked for (the library asks for the machine's
    parallelism), LZ4 buffers, one task per stream;
  - Storage Write on the official crate: 25,000 rows per Arrow request, about 5.5 MB, so a request
    stays under the 10 MB limit, and 8 requests in flight, which is the library's default window.
    The Arrow write of the library takes the same 25,000-row batches.

Python asks Storage Read for `max_stream_count = 0` by default, which lets BigQuery decide. It got
1 stream every time, while on the 1M-row table scan the library and the official crate got 4 of the
16 they asked for.

## Versions

| Client | Versions |
|---|---|
| bigquery (this library) | `master` after 0.5.0, commit `5f30c65`; gcloud-sdk 0.32.4, tonic 0.14.6, arrow 60.0.0 |
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
  Typed rows are decoded from Arrow with serde. Writes go to Storage Write as protobuf rows, or
  as Arrow for record batches.
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
as the other clients. The tables are from the query run.

`SELECT 1 AS x`:

| Client | Median | Range |
|---|---|---|
| bigquery | 0.117 s | 0.101-0.169 s |
| bigquery, `.job_creation_required()` | 0.198 s | 0.178-0.282 s |
| google-cloud-bigquery | 0.107 s | 0.091-0.109 s |
| Python | 0.270 s | 0.152-0.328 s |
| bq | 2.480 s | 2.390-2.524 s |

1,000 generated rows:

| Client | Median | Range | Rows/s |
|---|---|---|---|
| bigquery | 0.135 s | 0.111-0.169 s | 7,411 |
| bigquery, `.job_creation_required()` | 0.224 s | 0.197-0.284 s | 4,458 |
| google-cloud-bigquery | 0.126 s | 0.109-0.152 s | 7,935 |
| Python | 0.218 s | 0.181-0.271 s | 4,582 |
| bq | 2.488 s | 2.364-2.542 s | 402 |

The library and the official crate are about the same speed here, and both are faster than
Python, because of the job:

- The official crate sets `JobCreationMode::JobCreationOptional` on every query
  (`google-cloud-bigquery` 0.18.0, `src/query/builder.rs`, line 73), BigQuery's short query
  mode, where BigQuery may answer without creating a job. The library does the same by default.
  In these runs neither got a job back, while Python and the library's required mode got one
  every time.
- Creating the job is the whole difference: 81 ms on the constant query in the query run, and
  165 ms in the full run.
- The library was 10 ms behind the official crate in the query run and 41 ms in the full run
  (0.130 s against 0.089 s). To check whether that is the code, the 0.5.0 release, this commit
  and the official crate ran `SELECT 1` interleaved in one script, 20 rounds each, every round
  on a quiet machine: 0.094 s, 0.096 s and 0.091 s. So I think the gap comes from the network
  between runs.

About 0.6 s of every `bq` run is the CLI starting up (`bq version` timed in each run); the rest
probably is its job insert and polling, I didn't check.

## Large query result

200,000 generated rows of 4 columns, read as typed rows, from the query run:

| Client | Path | Median | Range | Rows/s |
|---|---|---|---|---|
| bigquery | Storage Read, 1 stream | 1.164 s | 1.137-1.328 s | 171,805 |
| bigquery, `.job_creation_required()` | Storage Read, 1 stream | 1.210 s | 1.079-1.227 s | 165,301 |
| google-cloud-bigquery | REST JSON pages | 4.387 s | 3.829-4.696 s | 45,585 |
| Python (`list(rows)`) | REST JSON pages | 4.550 s | 4.367-5.118 s | 43,954 |

A result this large always gets a job, so the two modes are the same here. The full run
gave 1.315 s, 3.779 s and 4.680 s. One run of the library in the full run took 4.550 s on the
same path, the others 1.218-1.429 s.

The same result as Arrow, from the full run:

| Client | Path | Median | Range | Rows/s |
|---|---|---|---|---|
| bigquery (`record_batches()`) | Storage Read, 1 stream | 1.175 s | 1.082-1.284 s | 170,243 |
| Python (`to_arrow()`) | Storage Read, 1 stream | 2.478 s | 2.283-2.797 s | 80,698 |
| google-cloud-bigquery | n/a: its query client returns JSON rows only | | | |

The library reads the destination table through Storage Read once the result does not fit in
the first response, while the official crate and Python iterate REST pages. The official crate
spent 0.018 s of its 3.8-4.7 s in `FromRow`, so nearly all of its time is the paging itself.

Python takes the same Storage Read path for `to_arrow()` and is still twice as slow. I guess it
is the extra metadata calls before the read session, but I didn't measure that.

## Table scan

The whole 1M-row table, every column, from the full run. MB/s is the table's 215 MB divided by
the median.

Typed rows:

| Client | Path | Median | Range | Rows/s | MB/s |
|---|---|---|---|---|---|
| bigquery (`obj::<T>()`) | Storage Read, 4 streams | 9.592 s | 8.622-10.090 s | 104,255 | 22.4 |
| Python (`to_dataframe()`) | Storage Read, 1 stream | 10.695 s | 9.741-11.309 s | 93,502 | 20.1 |
| google-cloud-bigquery (`FromRow`) | `SELECT *` query, REST JSON pages | 95.211 s | 94.464-97.281 s | 10,503 | 2.3 |

Arrow:

| Client | Path | Median | Range | Rows/s | MB/s |
|---|---|---|---|---|---|
| bigquery (`record_batches()`) | Storage Read, 4 streams | 9.475 s | 9.364-9.516 s | 105,539 | 22.7 |
| Python (`to_arrow()`) | Storage Read, 1 stream | 9.474 s | 9.254-9.624 s | 105,556 | 22.7 |
| google-cloud-bigquery (raw client + `arrow-ipc`) | Storage Read, 4 streams | 10.175 s | 9.680-10.344 s | 98,283 | 21.1 |

The official crate has no typed Storage Read, so its typed path is a query, and a `SELECT *`
query bills the table: 215 MB per run. Its `FromRow` took about 1.09 s of the 95 s.

Raw Arrow scans are about the same speed in all three clients, and the library has no advantage
there. The official crate was 7% slower in this run and 1% faster in the previous one, on
2026-10-04, so I don't count it. Every client moved about 150 MB per scan on the network
interface at 14-16 MB/s, so I think this connection is the limit, not the clients.

Python's `to_dataframe()` builds a pandas frame with typed columns, not row objects, so it is the
closest Python has to typed rows rather than the same thing.

## Storage Write

1M rows of the scan table's shape into the default stream, per run.

From Rust structures, from the full run:

| Client | Format | Median | Range | Rows/s | MB sent |
|---|---|---|---|---|---|
| bigquery (`insert().objects()`) | protobuf from serde | 33.976 s | 33.089-37.866 s | 29,432 | 180.5 |
| google-cloud-bigquery | Arrow built from the same rows | 35.433 s | 33.568-41.872 s | 28,222 | 221.4 |
| Python | n/a: its writer takes requests you build yourself, protobuf rows with a hand-made descriptor | | | | |

Both times include turning the Rust rows into the wire format. The library sent 18% fewer bytes
and was a bit faster, but the ranges overlap. On the network interface the library ran at about
5.8 MB/s and the official crate at about 6.7 MB/s, so the upload of this connection is probably
not the whole limit. I think the smaller requests are why the library is a bit faster, but that is
not measured.

From Arrow record batches, from the rerun:

| Client | Format | Median | Range | Rows/s | MB sent |
|---|---|---|---|---|---|
| bigquery (`insert().record_batches()`) | Arrow | 37.031 s | 34.438-37.606 s | 27,004 | 221.4 |
| google-cloud-bigquery | Arrow | 35.223 s | 33.516-38.493 s | 28,390 | 221.4 |

Both clients get the same 40 batches of 25,000 rows, built before the timer starts, so the times
are the IPC encoding and the appends. Both sent the same bytes, 40 requests each. The library was
5% slower here, and 6% slower in the full run's contended scenario too (38.958 s against
36.711 s), so it has no advantage for Arrow writes. I didn't find why.

## Decode cost

The scan table's 1M rows already in memory as Arrow batches, decoded into structs on one thread,
from the full run:

| Client | Median | Range | Rows/s |
|---|---|---|---|
| bigquery | 0.658 s | 0.639-0.670 s | 1,519,533 |
| google-cloud-bigquery | n/a | | |

The official crate has no Arrow to struct decoder. Its `FromRow` converts the JSON rows of a live
query, and `Row` has no public constructor, so it cannot be fed the same batches. Inside the REST
scan above its conversion took about 1.09 s per 1M rows, but on already parsed JSON values, so the
two numbers are not comparable.

In the library the decode runs on each read stream's task, next to the network reads, which is
probably why typed and Arrow scans take almost the same time.

## Cost

The full run billed 1.29 GB of queries, all of them the official crate's `SELECT *` scans. The
query run and the Arrow write rerun billed 0 bytes. Every other query read generated rows and
billed 0 bytes. The scans went through the Storage Read free tier, and the writes ingested about
215 MB per 1M rows under the Storage Write free tier.

## Reproducing

The harness is in [bench-compare](https://github.com/abdolence/bigquery-rs/tree/master/bench-compare),
an unpublished crate with the Rust contenders and a [uv](https://docs.astral.sh/uv/) project for
Python. You need application default credentials and optionally the `bq` CLI, found in `PATH` or
through `BQ_BIN`:

```sh
bench-compare/run.sh --project my-project
```

It creates the scratch dataset in `europe-north2` (`--location` changes it), runs everything and
deletes the dataset, also on failure. The raw results land in
`bench-compare/results/<run label>/results.json`, with the machine state of every run. A full run
takes about 45 minutes and costs a few cents. `--only query_const,query_1k,query_200k_rows` runs
just the query scenarios, `--only write_arrow` just the Arrow write. The summary of the runs above
is in `bench-compare/results-2026-10-05-europe-north2.json`, and the previous ones, on the code
before 0.5.0, in `bench-compare/results-2026-10-04-europe-north2.json`.
