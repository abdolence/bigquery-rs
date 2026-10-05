# Arrow record batches

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
