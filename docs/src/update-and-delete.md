# Updating and deleting

BigQuery tables are made for appending rows, but the library supports updates and deletes by
primary key too:

- `db.fluent().update()` writes whole rows: each replaces the row with its primary key, or is
  inserted if there is none;
- `db.fluent().delete()` deletes the rows with the given primary keys.

Both are written through BigQuery [change data capture](./cdc.md) (CDC), as upserts and
deletes on the table's default write stream. There is no `UPDATE` or `MERGE` statement to run,
BigQuery applies the changes itself.

## The table

The table needs a primary key, declared with `schema()`:

```rust,no_run
# use bigquery::*;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
struct Order {
    id: i64,
    customer: String,
    status: String,
    note: Option<String>,
}

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
db.fluent()
    .schema()
    .table(SHOP.table(ORDERS))
    .columns(|columns| {
        columns.fields([
            columns.field(path!(Order::id)).int64().required(),
            columns.field(path!(Order::customer)).string(),
            columns.field(path!(Order::status)).string(),
            columns.field(path!(Order::note)).string(),
        ])
    })
    .primary_key([path!(Order::id)])
    .sync()
    .await?;
# Ok(())
# }
```

BigQuery never enforces the key, it only uses it to find the row a change is for. So it is up
to you that the key is unique.

## Updates

```rust,no_run
# use bigquery::*;
# use serde::Serialize;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
#[derive(Serialize)]
struct Order {
    id: i64,
    customer: String,
    status: String,
    note: Option<String>,
}

# async fn example(db: BigQueryDb, orders: Vec<Order>) -> BigQueryResult<()> {
let order = Order {
    id: 42,
    customer: "Ada".to_string(),
    status: "shipped".to_string(),
    note: None,
};
db.fluent()
    .update()
    .in_table(SHOP.table(ORDERS))
    .object(&order)
    .execute()
    .await?;

// Many rows through one writer, applied in this order
db.fluent()
    .update()
    .in_table(SHOP.table(ORDERS))
    .objects(&orders)
    .execute()
    .await?;
# Ok(())
# }
```

An update replaces the whole row. There is no field mask, so a column you leave out or set to
`None` becomes `NULL`. To change one column, write the whole row with the new value. Be aware
that BigQuery does not support `UPDATE`, `DELETE` or `MERGE` statements on a table while CDC
changes are streamed to it, so a DML query is not a way around this.

## Deletes

A delete needs only the key columns, so a struct of just the key is enough:

```rust,no_run
# use bigquery::*;
# use serde::Serialize;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
#[derive(Serialize)]
struct OrderKey {
    id: i64,
}

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
db.fluent()
    .delete()
    .from(SHOP.table(ORDERS))
    .object(&OrderKey { id: 42 })
    .execute()
    .await?;

db.fluent()
    .delete()
    .from(SHOP.table(ORDERS))
    .objects([OrderKey { id: 1 }, OrderKey { id: 2 }])
    .execute()
    .await?;
# Ok(())
# }
```

The columns the key struct leaves out must be `NULLABLE`. A row without a `REQUIRED` column
does not serialize, so for a table with other `REQUIRED` columns pass the whole row instead.

## Ordering changes

Without sequence numbers BigQuery applies the changes to one key in the order it receives them.
That is fine for one producer at a time. If a change can arrive late or twice, give it a
sequence number, the highest one wins:

```rust,no_run
# use bigquery::*;
# use serde::Serialize;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
# #[derive(Serialize)]
# struct Order {
#     id: i64,
#     customer: String,
#     status: String,
#     note: Option<String>,
# }
# #[derive(Serialize)]
# struct OrderKey {
#     id: i64,
# }
# async fn example(db: BigQueryDb, order: Order) -> BigQueryResult<()> {
db.fluent()
    .update()
    .in_table(SHOP.table(ORDERS))
    .object(&order)
    .sequence_number(BigQueryChangeSequenceNumber::from(1041))
    .execute()
    .await?;

db.fluent()
    .delete()
    .from(SHOP.table(ORDERS))
    .object(&OrderKey { id: 42 })
    .sequence_number(BigQueryChangeSequenceNumber::from(1042))
    .execute()
    .await?;
# Ok(())
# }
```

`.sequence_number(..)` is there only for a single `.object(..)`. Rows in one `.objects(..)` would
all share it, and two changes to one key with the same number have no defined order. For many
changes, each with its own number, use `.changes(..)` on an insert, see
[change data capture](./cdc.md#writing-changes). Once a key has changes with sequence numbers,
send one with every later change to it.

## When the changes show up

BigQuery applies the changes in the background, and the table's `max_staleness` option says how
old the applied data may be:

- without `max_staleness`, a query merges the changes not applied yet at query time, so it sees
  every change as soon as `execute()` returns, and pays for that merge;
- with `max_staleness`, say 10 minutes, a query may read data up to 10 minutes old, so a read
  right after an update may not show it yet.

More on `max_staleness`, its costs and the limits of CDC tables in
[change data capture](./cdc.md).

Each `execute()` opens a CDC writer and finishes it, which costs a round trip or so. For a
steady stream of changes keep one writer open with `db.create_cdc_writer(..)` instead.

Full example available [here](https://github.com/abdolence/bigquery-rs/blob/master/examples/update-and-delete.rs).
