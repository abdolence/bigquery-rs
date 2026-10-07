# Change data capture

BigQuery tables are made for appending rows. Change data capture (CDC) is BigQuery's way to keep
a table in sync with a changing source instead: each row you write is a change to the row with
the same primary key, an upsert or a delete, and BigQuery applies the changes for you. There is no
`MERGE` or `UPDATE` statement to run and no staging table to clean up.

The typical use is mirroring an OLTP database into BigQuery as a stream. You read the changes of
a PostgreSQL or MySQL table from its log, and write each one as it comes. The BigQuery table then
looks like the source table, with a delay you choose.

This chapter is about the CDC API itself. To update or delete a few rows by primary key,
`db.fluent().update()` and `db.fluent().delete()` are simpler, see
[updating and deleting](./update-and-delete.md). When to use CDC and when to run `UPDATE` and
`DELETE` statements instead, see [CDC or DML queries](./update-and-delete.md#cdc-or-dml-queries).

## How it works

A CDC table needs a primary key. BigQuery's keys are `NOT ENFORCED`: BigQuery never checks them,
so it is up to you that the key is unique in the source. A key can have up to 16 columns.

Every row written through CDC carries two pseudo-columns besides the table's own columns:

- `_CHANGE_TYPE`: `UPSERT` inserts the row, or replaces the row with the same key; `DELETE`
  deletes the row with the same key, and only its key columns matter;
- `_CHANGE_SEQUENCE_NUMBER`, optional: orders the changes to one key.

Without a sequence number BigQuery orders the changes to one key by the time it received them,
the latest wins. That is fine for one producer writing in order, but rows can be resent after a
reconnect, and two producers can race. With a sequence number the highest one wins, whenever it
arrived, so an old change that arrives late or twice does not overwrite a newer one.

A sequence number is up to four sections of hexadecimal digits separated by `/`, each up to 16
digits, from `0` to `FFFFFFFFFFFFFFFF/FFFFFFFFFFFFFFFF/FFFFFFFFFFFFFFFF/FFFFFFFFFFFFFFFF`.
BigQuery compares the sections as numbers, from left to right. A PostgreSQL LSN such as
`16/B374D848` is already in this form. Between two changes with the same number, the one
BigQuery ingested last wins. Sequence numbers are a choice for the whole table: once a table gets
changes with sequence numbers, send one with every change to it, mixing changes with and without
them gives an unpredictable order.

BigQuery does not rewrite the table on every change. It keeps the recent changes beside the table
and applies them in the background. The table's `max_staleness` option says how old the applied
data may be:

- without `max_staleness`, a query merges the changes not applied yet at query time, so it always
  sees the latest data and pays for that merge;
- with `max_staleness = INTERVAL 10 MINUTE`, BigQuery applies the changes at least once every 10
  minutes with background jobs, and a query reads the applied table, which can be up to 10 minutes
  old. If the background jobs fall behind the interval, queries merge at query time again.

Either way a reader sees the merged result, never the raw changes. The pseudo-columns cannot be
queried.

## Creating the table

Declare the primary key with `schema()`, and set `max_staleness` with a DDL statement:

```rust,no_run
# use bigquery::*;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const CUSTOMERS: BigQueryTableId = BigQueryTableId::from_static("customers");
struct Customer {
    id: i64,
    name: String,
    city: Option<String>,
}

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
db.fluent()
    .schema()
    .table(SHOP.table(CUSTOMERS))
    .columns(|columns| {
        columns.fields([
            columns.field(path!(Customer::id)).int64().required(),
            columns.field(path!(Customer::name)).string(),
            columns.field(path!(Customer::city)).string(),
        ])
    })
    .primary_key([path!(Customer::id)])
    .cluster_by([path!(Customer::id)])
    .sync()
    .await?;

db.fluent()
    .query("ALTER TABLE shop.customers SET OPTIONS (max_staleness = INTERVAL 10 MINUTE)")
    .execute()
    .await?;
# Ok(())
# }
```

Clustering by the key is what Google's own CDC example does; it is not required.

## Writing changes

`.changes(..)` on an insert takes `BigQueryChange`s, each with a `BigQueryChangeType` and an
optional `BigQueryChangeSequenceNumber`:

```rust,no_run
# use bigquery::*;
# use serde::Serialize;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const CUSTOMERS: BigQueryTableId = BigQueryTableId::from_static("customers");
#[derive(Serialize)]
struct Customer {
    id: i64,
    name: String,
    city: Option<String>,
}

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let changes = vec![
    BigQueryChange {
        change_type: BigQueryChangeType::Upsert,
        sequence_number: Some(BigQueryChangeSequenceNumber::from(1)),
        row: Customer {
            id: 1,
            name: "Ada".to_string(),
            city: Some("Stockholm".to_string()),
        },
    },
    BigQueryChange {
        change_type: BigQueryChangeType::Delete,
        sequence_number: Some("16/B374D848".parse()?),
        row: Customer {
            id: 2,
            name: String::new(),
            city: None,
        },
    },
];

db.fluent()
    .insert()
    .into(SHOP.table(CUSTOMERS))
    .changes(changes)
    .execute()
    .await?;

// Or plain rows as upserts, for a table that does not use sequence numbers
let customer = Customer {
    id: 3,
    name: "Grace".to_string(),
    city: None,
};
db.fluent()
    .insert()
    .into(SHOP.table(CUSTOMERS))
    .object(&customer)
    .upsert()
    .execute()
    .await?;
# Ok(())
# }
```

`BigQueryChangeSequenceNumber::from(u64)` writes the number in hex, as one section. Parsing a
string takes the text as it is and BigQuery checks its form, so a source that has its own
sequence text, like the LSN above, can pass it through.

For a long-running stream of changes, `db.create_cdc_writer(..)` opens a CDC writer, the same as
the [streaming writer](./writing-data.md#streaming-writer) with its batching, backpressure and
response stream:

```rust,no_run
# use bigquery::*;
# use serde::Serialize;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const CUSTOMERS: BigQueryTableId = BigQueryTableId::from_static("customers");
# #[derive(Serialize)]
# struct Customer {
#     id: i64,
#     name: String,
#     city: Option<String>,
# }
# async fn example(db: BigQueryDb, source_changes: Vec<BigQueryChange<Customer>>) -> BigQueryResult<()> {
let (mut writer, _responses) = db
    .create_cdc_writer::<Customer>(
        SHOP.table(CUSTOMERS),
        BigQueryStreamingWriteOptions::new(),
    )
    .await?;

for change in &source_changes {
    writer.write_change(change).await?;
}

let summary = writer.finish().await?;
println!("{} changes written", summary.rows_written);
# Ok(())
# }
```

`writer.upsert(&row)` and `writer.delete(&row)` write a change without a sequence number.

Full example available [here](https://github.com/abdolence/bigquery-rs/blob/master/examples/cdc-upsert.rs).

## Why protobuf

BigQuery takes CDC changes only as protobuf rows on the default write stream, Apache Arrow rows
are not supported for CDC. This is the main reason the library writes every row as protobuf: one
encoder serves plain inserts and CDC alike. Protobuf requests were also smaller than Arrow for the
same rows in the [benchmarks](./benchmarks.md), 18% fewer bytes for 1M rows.

## Limits

From BigQuery's side, as its
[CDC limitations](https://cloud.google.com/bigquery/docs/change-data-capture#limitations) list them:

- CDC goes only through the default stream, so `.exactly_once()` and `.atomic()` cannot be
  combined with it; the library refuses them with `InvalidParametersError` before any request;
- the primary key is not enforced and has at most 16 columns;
- the table has at most 2,000 top-level columns;
- mutating DML (`UPDATE`, `DELETE`, `MERGE`), wildcard table queries and search indexes are not
  supported on the table;
- while queries merge at query time because `max_staleness` is too low or not set, the table cannot
  be copied, cloned or snapshotted, and cannot be read through the Storage Read API, so
  [table reads](./reading-tables.md) need a `max_staleness` the background jobs keep up with;
- a query that merges at query time scans the whole table, whatever its partition filter;
- exports do not include changes not applied yet;
- the applying is BigQuery compute and is billed as such, on demand unless you have a BACKGROUND
  reservation, which Standard edition does not have.

From the library's side:

- a CDC writer always writes `_CHANGE_TYPE`, so write plain inserts to the same table with a
  separate writer. BigQuery's CDC documentation says rows with and without a change type on one
  connection are not supported;
- a delete serializes a whole row, but only its key columns matter. A struct of just the key
  works when every column it leaves out is `NULLABLE`;
- delivery is at least once, as on every default stream write. With sequence numbers a change
  sent twice is harmless.
