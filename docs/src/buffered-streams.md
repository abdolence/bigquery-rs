# Buffered streams

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
