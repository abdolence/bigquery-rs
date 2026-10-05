# Writing data

The library writes rows through the BigQuery Storage Write API. Rows are your structures,
serialized with serde straight into protobuf against the table's schema, so there is no JSON and
no schema to declare on the client. If your data is in Arrow already, the library writes raw
record batches too, see [Arrow record batches](#arrow-record-batches).

There are two ways to write:

- `db.fluent().insert()` for rows you already have: it opens a writer, writes every row, finishes
  and returns a summary;
- `db.create_streaming_writer()` for a long-running producer: one writer you keep open and write
  rows to as they come.

Both batch the rows by themselves, so you never need to batch before writing.

## Inserts

```rust,no_run
# use bigquery::*;
# use serde::Serialize;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
#[derive(Serialize)]
struct Order {
    id: i64,
    customer: String,
    placed_at: jiff::Timestamp,
}

# async fn example(db: BigQueryDb, orders: Vec<Order>) -> BigQueryResult<()> {
// One row
db.fluent()
    .insert()
    .into(SHOP.table(ORDERS))
    .object(&orders[0])
    .execute()
    .await?;

// Many rows
let summary = db
    .fluent()
    .insert()
    .into(SHOP.table(ORDERS))
    .objects(&orders)
    .execute()
    .await?;
println!("{} rows written", summary.rows_written);
# Ok(())
# }
```

`objects(..)` takes anything iterable whose items are `Serialize`, so a `Vec`, a slice or an
iterator over rows built on the fly all work. `execute()` returns the first failed batch's error,
or a `BigQueryWriteSummary` with the rows written, the batches and the bytes sent. A row that does
not serialize stops it with `SerializeError`. The rows before it may or may not be written: only
the requests already sent can land, and the rows still waiting in a batch are dropped. If you need
all rows or none, use `.atomic()` or check the rows before the insert.

Every `execute()` opens a write stream first, which is a round trip of about 300 ms. That is fine
for a load of many rows, but for many small writes keep one streaming writer instead.

Full example available [here](https://github.com/abdolence/bigquery-rs/blob/master/examples/insert.rs).

## Streaming writer

```rust,no_run
# use bigquery::*;
# use futures::StreamExt;
# use serde::Serialize;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
# #[derive(Serialize)]
# struct Order {
#     id: i64,
#     customer: String,
# }
# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let (mut writer, mut responses) = db
    .create_streaming_writer::<Order>(SHOP.table(ORDERS))
    .await?;

// Reading the responses is optional, one item per batch
let responses_task = tokio::spawn(async move {
    while let Some(response) = responses.next().await {
        match response {
            Ok(written) => println!("batch {} written", written.batch_index),
            Err(err) => eprintln!("batch failed: {err}"),
        }
    }
});

for id in 0..10_000 {
    let order = Order {
        id,
        customer: format!("customer-{id}"),
    };
    writer.write(&order).await?;
}

let summary = writer.finish().await?;
println!("{} written, {} failed", summary.rows_written, summary.rows_failed);
let _ = responses_task.await;
# Ok(())
# }
```

The writer owns one `AppendRows` connection, run by a background task. It is `Send` but not
`Clone`, so open more writers if you need more connections. `write_all(..)` writes several rows in
order.

The response stream yields one `BigQueryWriteResponse` or one error per batch, in batch order, and
ends when the writer finishes. Reading it is optional, `finish()` reports the failed rows either
way.

Be aware not to just drop the writer instead of calling `finish()`. That logs a warning, drops the
rows not acknowledged yet and leaves a pending stream uncommitted.

`db.create_streaming_writer_with_options(..)` takes `BigQueryStreamingWriteOptions`, the same
options as `.options(..)` on an insert.

Full example available [here](https://github.com/abdolence/bigquery-rs/blob/master/examples/streaming-writer.rs).

## Built-in batching

Rows are encoded as you write them and collected into a batch, one `AppendRows` request. A batch
is sent when one of these comes first:

- **size:** the next row would take it over `max_request_bytes`, 19,000,000 bytes by default;
- **time:** `max_batch_delay` has passed since its first row, 100 ms by default, so the rows of a
  producer that went quiet still go out;
- **rows:** it holds `max_batch_rows` rows, if you set it, no limit by default;
- `flush()` or `finish()` is called.

`max_request_bytes` can be lowered, never raised. BigQuery's limit is 20 MiB per request, and a
request over it is not rejected on its own: it ends the whole connection. So the library counts
every request's exact size before sending it. A single row too large for a request alone fails
with `SerializeError` of kind `RowTooLarge` and is never sent.

```rust,no_run
# use bigquery::*;
# use serde::Serialize;
# use std::time::Duration;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
# #[derive(Serialize)]
# struct Order {
#     id: i64,
# }
# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let (writer, _responses) = db
    .create_streaming_writer_with_options::<Order>(
        SHOP.table(ORDERS),
        BigQueryStreamingWriteOptions::new()
            .with_max_batch_delay(Duration::from_millis(500))
            .with_max_batch_rows(10_000),
    )
    .await?;
# let _ = writer;
# Ok(())
# }
```

## Backpressure

The writer does not wait for one batch to be acknowledged before sending the next, it pipelines
them. Two limits bound what is sent and not acknowledged yet:

- `max_inflight_requests`: 8 by default;
- `max_inflight_bytes`: 64 MiB by default.

When either is reached, `write().await` waits until BigQuery acknowledges a batch. So a producer
faster than the network slows down to its speed instead of buffering rows in memory.

## Flush and finish

- `flush()` sends the open batch and waits until every batch written so far has an outcome. It
  fails only when the writer itself failed for good; a failed batch is reported on the response
  stream and in the summary.
- `finish()` flushes, waits for every acknowledgement and closes the writer, then returns the
  `BigQueryWriteSummary`. On the default stream that is all; a committed stream is finalized, a
  buffered one is flushed up to its last row and finalized, and a pending one is finalized and
  committed.

## Write modes

The mode decides which write stream the rows go through, and with it the delivery guarantee:

| Mode | Insert | Writer option | Rows visible | Guarantee |
|---|---|---|---|---|
| Default | `.objects(..)` | `BigQueryWriteMode::Default` | as soon as each batch is acknowledged | at least once: a batch resent after a reconnect can be stored twice |
| Exactly once | `.exactly_once()` | `BigQueryWriteMode::Committed` | as soon as each batch is acknowledged | exactly once: every request has an offset, and BigQuery recognises a resent one |
| Atomic | `.atomic()` | `BigQueryWriteMode::Pending` | all together, at the commit | all rows or none |
| Buffered | `.buffered()` | `BigQueryWriteMode::Buffered` | up to the offset you flush | exactly once, as committed |
| CDC | `.changes(..)` or `.upsert()` | default stream | after BigQuery applies the changes | upserts and deletes by primary key, see [change data capture](./cdc.md) |

```rust,no_run
# use bigquery::*;
# use serde::Serialize;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
# #[derive(Serialize)]
# struct Order {
#     id: i64,
# }
# async fn example(db: BigQueryDb, orders: Vec<Order>) -> BigQueryResult<()> {
// Every row exactly once, even when a request is resent
db.fluent()
    .insert()
    .into(SHOP.table(ORDERS))
    .objects(&orders)
    .exactly_once()
    .execute()
    .await?;

// Every row becomes visible at one commit, or none does
let summary = db
    .fluent()
    .insert()
    .into(SHOP.table(ORDERS))
    .objects(&orders)
    .atomic()
    .execute()
    .await?;
println!("committed at {:?}", summary.commit_time);
# Ok(())
# }
```

`.options(..)` replaces the options and the mode with them, so call it before `.exactly_once()`
or `.atomic()`.

The atomic mode is what a "batch write" in the transactional sense is. If any batch fails,
`finish()` returns `WriteStreamError` with the code `NOT_COMMITTED` and the table gets nothing.

Several pending writers on one table can commit together. `finalize()` instead of `finish()`
leaves the commit to `db.commit_write_streams(..)`:

```rust,no_run
# use bigquery::*;
# use serde::Serialize;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
# #[derive(Serialize)]
# struct Order {
#     id: i64,
# }
# async fn example(db: BigQueryDb, first: Vec<Order>, second: Vec<Order>) -> BigQueryResult<()> {
let options = BigQueryStreamingWriteOptions::new().with_mode(BigQueryWriteMode::Pending);

let (mut first_writer, _) = db
    .create_streaming_writer_with_options::<Order>(SHOP.table(ORDERS), options.clone())
    .await?;
let (mut second_writer, _) = db
    .create_streaming_writer_with_options::<Order>(SHOP.table(ORDERS), options)
    .await?;

first_writer.write_all(&first).await?;
second_writer.write_all(&second).await?;

let streams = vec![first_writer.finalize().await?, second_writer.finalize().await?];
let commit_time = db.commit_write_streams(streams).await?;
println!("committed at {commit_time}");
# Ok(())
# }
```

## Buffered streams

A buffered stream keeps the rows it acknowledged invisible until you flush them. A flush makes
every row up to an offset readable, and a later flush moves that offset further. It suits a
producer that has to decide when its rows count, for example only after it saved its own
checkpoint:

- `flush_rows()` sends the open batch, waits for every acknowledgement and flushes the stream up
  to its last row. It returns the flushed offset, or `None` while the stream has no rows;
- `flush_rows_to(offset)` flushes up to and including `offset`, the stream offset of a row. The
  last row of an acknowledged batch is at `offset + row_count - 1` of its
  `BigQueryWriteResponse`. It does not wait for the batches in flight, and BigQuery checks the
  offset.

```rust,no_run
# use bigquery::*;
# use serde::Serialize;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
# #[derive(Serialize)]
# struct Order {
#     id: i64,
# }
# async fn save_checkpoint(offset: i64) {}
# async fn example(db: BigQueryDb, orders: Vec<Order>) -> BigQueryResult<()> {
let (mut writer, _responses) = db
    .create_streaming_writer_with_options::<Order>(
        SHOP.table(ORDERS),
        BigQueryStreamingWriteOptions::new().with_mode(BigQueryWriteMode::Buffered),
    )
    .await?;

for chunk in orders.chunks(1_000) {
    writer.write_all(chunk).await?;
    // The rows of this chunk become readable here, not before
    if let Some(offset) = writer.flush_rows().await? {
        save_checkpoint(offset).await;
    }
}

let summary = writer.finish().await?;
println!("{} rows written", summary.rows_written);
# Ok(())
# }
```

`finish()` flushes the rest before it finalizes the stream, so every row you wrote becomes
readable. Finalizing alone does not flush, and only flushed rows are readable. If you need the
unflushed rows to stay unread, drop the writer instead of finishing it; it logs a warning and the
stream is never finalized.

After the writer failed for good, `flush_rows_to(..)` still works, since it does not need the
connection. Flush to the last acknowledged row to make every acknowledged row readable.

`.buffered()` on an insert writes through a buffered stream and flushes once at the end. Unlike
`.atomic()`, a failed batch does not hold back the others.

Google itself calls the buffered type an advanced one, for the Apache Beam connector mostly. If
you only need a few rows to appear together, the exactly once mode with all of them in one batch
does that too.

## Arrow record batches

The library writes Arrow record batches as they are, the way reads return them with
`.record_batches()`. Every write mode works the same as for structures:

- `.record_batches(..)` on an insert, with `.exactly_once()`, `.atomic()`, `.buffered()` and
  `.options(..)`;
- `db.create_record_batch_writer(..)` for a long-running producer, with `write_batch(..)` and the
  same `flush()`, `finish()`, `finalize()`, `flush_rows()` and `flush_rows_to(..)`.

```rust,no_run
# use bigquery::*;
# use bigquery::arrow_array::{Int64Array, RecordBatch, StringArray};
# use bigquery::arrow_schema::{DataType, Field, Schema};
# use std::sync::Arc;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
# async fn example(db: BigQueryDb) -> Result<(), Box<dyn std::error::Error>> {
let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("customer", DataType::Utf8, true),
]));
let batch = RecordBatch::try_new(
    schema,
    vec![
        Arc::new(Int64Array::from(vec![1, 2])),
        Arc::new(StringArray::from(vec!["customer-1", "customer-2"])),
    ],
)?;

// An insert, here exactly once
db.fluent()
    .insert()
    .into(SHOP.table(ORDERS))
    .record_batches([batch.clone()])
    .exactly_once()
    .execute()
    .await?;

// A streaming writer, here buffered
let (mut writer, _responses) = db
    .create_record_batch_writer_with_options(
        SHOP.table(ORDERS),
        BigQueryStreamingWriteOptions::new().with_mode(BigQueryWriteMode::Buffered),
    )
    .await?;
writer.write_batch(&batch).await?;
writer.flush_rows().await?;
let summary = writer.finish().await?;
println!("{} rows written", summary.rows_written);
# Ok(())
# }
```

Each batch goes with its own schema, and the library does not check it against the table, since
BigQuery does. A batch that does not fit the table fails as a batch, on the response stream and
in the summary. Google lists how Arrow types map to BigQuery types in
[supported data types](https://cloud.google.com/bigquery/docs/supported-data-types). For
example, a TIMESTAMP column takes `Timestamp(Microsecond, "UTC")`, a DATETIME column the same
without a time zone, and a NUMERIC column `Decimal128`.
BigQuery refuses dictionary-encoded columns, so cast them to their value type first, with
`arrow::compute::cast` for instance.

The batching is a bit different from structures:

- a record batch is sent as it is written, it never shares a request with another one, so
  `max_batch_delay` does not apply. Many tiny batches are as many requests;
- a batch larger than `max_request_bytes` is sent as slices of it, one request each. The library
  serializes every slice to measure it, so no request goes over the limit. A single row too large
  for a request alone fails with `SerializeError` of kind `RowTooLarge`;
- `max_batch_rows` caps the rows of every slice;
- a batch with another schema than the one before it opens a new connection, since BigQuery reads
  an Arrow schema from the first request of a connection only.

CDC writes take structures only, since the change columns have to go as protobuf.

Full example available [here](https://github.com/abdolence/bigquery-rs/blob/master/examples/record-batch-writes.rs).

## Errors

- A row that does not fit the table's schema fails a streaming writer's `write()` with
  `SerializeError`, naming the row by its index in write order. Nothing of it is sent, and the
  writer goes on.
- `insert().execute()` stops at the first row that does not serialize and returns
  `SerializeError`. On the default and committed streams the earlier rows may or may not be
  written: the requests already sent stay written, and the rows still in the open or queued
  batches are dropped. With the default batch size a short insert usually has all of its rows in
  one open batch, so nothing is written. A pending stream (`.atomic()`) commits nothing.
- A batch BigQuery rejects is `RowErrors` with every row BigQuery named, by its index in write
  order, so you can find and resend them. None of the batch's rows are written, and the other
  batches are not affected.
- A retryable connection failure makes the writer reconnect and resend every batch not
  acknowledged yet, up to the client's `max_retries`. In the default mode that is where a row can
  be stored twice.
- Once the writer failed for good, every `write()` returns that error.

## Schema changes

The writer follows the table's schema while it runs. A column added to the table is picked up for
the next batches, from BigQuery's own notice or when a row has a field the writer did not know.

Be aware not to drop a column while a writer still sends it. BigQuery keeps accepting the values
for about 9 seconds and drops them silently, before it starts rejecting the rows. Stop every
writer from sending the column first, then drop it.

A field a row leaves out is stored as NULL by default. `missing_value` with
`BigQueryMissingValue::DefaultValue` asks BigQuery to use the column's default value expression
instead. The library sends it as BigQuery's `default_missing_value_interpretation` and has no live
test for it yet.

## Caveats

- A large `.atomic()` load stays invisible, and holds its rows in a pending stream, until it is
  committed. It suits a single load with a clear end. For an endless stream use the default or
  the exactly once mode.
- Rows from two writers on one table can interleave, nothing orders rows between writers.
- Only a CDC sequence number orders changes to the same key, see
  [change data capture](./cdc.md).
- Every insert pays the round trip to open its write stream; share a streaming writer for many
  small writes.

The [benchmarks](./benchmarks.md) have a Storage Write run of 1M rows against the official crate.
