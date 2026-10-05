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

// Many rows through one writer
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

Without sequence numbers, of two rows with one key the row BigQuery ingested last wins, see
[ordering changes](#ordering-changes).

## Deletes

A delete needs only the primary key. `.key(..)` takes the key value itself: a plain value for a
key of one column, a tuple in the key's column order for a key of several:

```rust,no_run
# use bigquery::*;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
# const ORDER_LINES: BigQueryTableId = BigQueryTableId::from_static("order_lines");
# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
db.fluent()
    .delete()
    .from(SHOP.table(ORDERS))
    .key(42)
    .execute()
    .await?;

// The primary key of order_lines is (order_id, line)
db.fluent()
    .delete()
    .from(SHOP.table(ORDER_LINES))
    .keys([(42, "line-1"), (42, "line-2")])
    .execute()
    .await?;
# Ok(())
# }
```

The library reads the key's columns from the table's metadata, one `GetTable` call per
`execute()`, and writes rows with only those columns. A table without a primary key, or a tuple
with another number of values than the key has columns, fails with `InvalidParametersError`
before anything is written. A value for a key of one column is written as it is, so a `Vec<u8>`
works for a `BYTES` key, and a value of the wrong type fails at the write with
`SerializeError`.

A row works as well, `.object(..)` and `.objects(..)` take a struct of just the key, or the whole
row:

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

The columns the key struct or `.key(..)` leaves out must be `NULLABLE`. A row without a
`REQUIRED` column does not serialize, so for a table with other `REQUIRED` columns pass the whole
row instead.

## Ordering changes

Without sequence numbers, of the changes to one key the one BigQuery ingested last wins. That is
fine for one producer at a time. If a change can arrive late or twice, give it a sequence
number, the highest one wins:

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
# async fn example(db: BigQueryDb, order: Order, orders: Vec<Order>) -> BigQueryResult<()> {
db.fluent()
    .update()
    .in_table(SHOP.table(ORDERS))
    .object(&order)
    .sequence_number(BigQueryChangeSequenceNumber::from(1041))
    .execute()
    .await?;

// One number for the rows of one source transaction
db.fluent()
    .update()
    .in_table(SHOP.table(ORDERS))
    .objects(&orders)
    .sequence_number(BigQueryChangeSequenceNumber::from(1042))
    .execute()
    .await?;

db.fluent()
    .delete()
    .from(SHOP.table(ORDERS))
    .key(42)
    .sequence_number(BigQueryChangeSequenceNumber::from(1043))
    .execute()
    .await?;
# Ok(())
# }
```

`.sequence_number(..)` gives the same number to every row of the call. Between changes to one key
with the same number, the one BigQuery ingested last wins. For changes each with its own number,
use `.changes(..)` on an insert, see [change data capture](./cdc.md#writing-changes).

Be aware that sequence numbers are a choice for the whole table: once a table takes changes with
sequence numbers, send one with every change to it. Mixing changes with and without them gives
an unpredictable order.

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
