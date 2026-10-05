# Datasets, tables and jobs

Besides the declarative table schemas, the library provides the plain admin calls over the v2 API:

- create, read, update, delete and list datasets;
- read, delete and list tables;
- read, delete, cancel and list jobs.

Every call returns the library's own types, such as `BigQueryDataset`, `BigQueryTable` and
`BigQueryJob`, and a value BigQuery leaves out is `None`.

## Dataset and table IDs

Every call that names a dataset or a table takes the validated ID types, `BigQueryDatasetId` and
`BigQueryTableId`. Declare the ones your application knows up front as constants, and build
references from them:

```rust
use bigquery::*;

const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");

// The table in the client's project
let orders = SHOP.table(ORDERS);
assert_eq!(orders.to_string(), "shop.orders");

// The same table in another project
let other = BigQueryDatasetRef::new("acme-prod", SHOP)?.table(ORDERS);
assert_eq!(other.to_string(), "acme-prod.shop.orders");
# Ok::<(), bigquery::errors::BigQueryError>(())
```

`from_static` checks the literal at compile time, so an invalid one in a `const` fails the build.
An ID that arrives at run time goes through `new`, `parse()` or `try_into()`, and deserializing an
ID checks it the same way:

```rust
# use bigquery::*;
let dataset = BigQueryDatasetId::new("shop_eu")?;
let table: BigQueryTableId = "orders_2026".parse()?;
let orders = dataset.table(table);
# let _ = orders;

assert!(BigQueryDatasetId::new("shop.eu").is_err());
# Ok::<(), bigquery::errors::BigQueryError>(())
```

The check is deliberately narrow. An ID is rejected when it is empty, longer than 1,024 bytes, or
holds a control character or any of `` ` ``, `'`, `"`, `\`, `.`, `/`, `$` and `@`, since those
would change a resource path, a dotted reference or SQL text built from the ID. BigQuery's full
naming rules are left to BigQuery, so an ID that passes here can still be refused by the call that
sends it.

`BigQueryTableRef` and `BigQueryDatasetRef` also parse from text, `dataset.table` or
`project.dataset.table`. Be aware not to parse text you do not trust this way, since the text picks
the project. Build the reference from the IDs instead, or check that `project()` is `None` after
parsing.

The project ID stays a plain `String`, because it is shared by every Google Cloud product. It is
checked only for being non-empty and free of `/` and control characters. A location is a
`BigQueryLocation`, such as `BigQueryLocation::from_static("EU")`, checked only for being
non-empty.

## Datasets

Datasets are reached through `db.fluent().schema().dataset(..)`, which takes a `BigQueryDatasetId`
for the client's project or a `BigQueryDatasetRef` for another one:

```rust,no_run
# use bigquery::*;
# use std::time::Duration;
const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
const EU: BigQueryLocation = BigQueryLocation::from_static("EU");

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let shop = db
    .fluent()
    .schema()
    .dataset(SHOP)
    .create()
    .location(EU)
    .description("Shop data")
    .default_table_expiration(Duration::from_secs(90 * 24 * 60 * 60))
    .labels([("team", "shop")])
    .execute()
    .await?;
println!("created {} in {:?}", shop.reference, shop.location);

let shop = db.fluent().schema().dataset(SHOP).get().await?;
println!("{:?}", shop.labels.get("team"));
# Ok(())
# }
```

The location cannot change after the dataset is created. Unset, BigQuery picks `US`. Creating a
dataset that already exists fails with `DataConflictError`.

`update()` changes the description and the labels, and keeps everything else as the dataset has
it:

```rust,no_run
# use bigquery::*;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let shop = db
    .fluent()
    .schema()
    .dataset(SHOP)
    .update()
    .label("stage", "prod")
    .remove_label("tmp")
    .clear_description()
    .execute()
    .await?;
# let _ = shop;
# Ok(())
# }
```

- `description(..)` sets the description, `clear_description()` removes it;
- `label(key, value)` adds or changes one label, `remove_label(key)` removes one, and `labels(..)`
  replaces all of them. Label changes apply in the order they are made.

The update reads the dataset and writes it back with the etag it read as an `if-match`
precondition, so a dataset that changed in between fails with `DataConflictError` and nothing is
written. Run it again. Only the metadata is written, the access list stays as it is.

There are two ways to delete a dataset:

- `delete()` deletes an empty dataset. BigQuery refuses one that holds any table, view, model or
  routine, and nothing is deleted;
- `dangerously_delete_with_contents()` deletes every table in the dataset with every row in it,
  and its views, models and routines, then the dataset. Nothing is kept for an undo.

```rust,no_run
# use bigquery::*;
# const SCRATCH: BigQueryDatasetId = BigQueryDatasetId::from_static("scratch");
# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
db.fluent()
    .schema()
    .dataset(SCRATCH)
    .dangerously_delete_with_contents()
    .await?;
# Ok(())
# }
```

## Listing datasets and tables

Listings are streams that fetch the next page when the stream reaches it:

```rust,no_run
# use bigquery::*;
use futures::StreamExt;

# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let mut datasets = db.fluent().schema().datasets().stream_all().await?;
while let Some(dataset) = datasets.next().await {
    println!("{} in {:?}", dataset.reference, dataset.location);
}

let tables: Vec<BigQueryTableSummary> = db
    .fluent()
    .schema()
    .dataset(SHOP)
    .tables()
    .page_size(100)
    .stream_all()
    .await?
    .collect()
    .await;
# let _ = tables;
# Ok(())
# }
```

`stream_all()` logs a failed page at `error` and ends there, since the listing cannot go on without
the token that page would have returned. `stream_all_with_errors()` yields the failure as the
stream's last item instead. `page_size(..)` sets how many items one call returns, and
`datasets().project(..)` lists another project's datasets.

The listings return summaries, `BigQueryDatasetSummary` and `BigQueryTableSummary`, with what
BigQuery's list calls return: the reference, the location or table type, the labels, etc. Read the
full value with `get()`.

Full example available [here](https://github.com/abdolence/bigquery-rs/blob/master/examples/datasets-admin.rs).

## Tables

Tables are created and changed through the declarative schemas, see
[Schema management](./schema-management.md). The same builder has two plain calls, which act on the
table and ignore anything declared before them:

```rust,no_run
# use bigquery::*;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let orders = db.fluent().schema().table(SHOP.table(ORDERS)).get().await?;
for field in &orders.schema.fields {
    println!("{} {} {}", field.name, field.field_type, field.mode);
}
println!("{:?} rows, partitioning {:?}", orders.num_rows, orders.partitioning);

db.fluent().schema().table(SHOP.table(ORDERS)).delete().await?;
# Ok(())
# }
```

`get()` returns a `BigQueryTable`: the reference, the `table_type`, the schema as
`BigQueryTableSchema`, the description, labels, partitioning, clustering, `num_rows`, `num_bytes`,
the location, and the creation, last modified and expiration times. It reads views, materialized
views, external tables and snapshots as well. The schema uses the same types as the
[type mapping](./types.md), so `INTEGER` from the v2 API reads as `BigQueryFieldType::Int64`.

Be aware `num_rows` and `num_bytes` lag Storage Write: rows written through committed or pending
streams show after about a minute, and rows on the default stream can take more than 5 minutes. A
`SELECT COUNT(*)` gives the current count.

`delete()` deletes the table with every row in it. Nothing is kept for an undo, and open writers to
it fail.

## Jobs

Every query that runs as a job names it in its outcome, as a `BigQueryJobRef` with the project,
the job ID and the location. The job calls are methods on `BigQueryDb`:

```rust,no_run
# use bigquery::*;
# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let outcome = db
    .fluent()
    .query("SELECT 1")
    .job_creation_required()
    .execute()
    .await?;

if let Some(job_ref) = &outcome.job {
    let job = db.get_job(job_ref).await?;
    println!(
        "{:?} {:?}, billed {:?} bytes",
        job.statement_type, job.state, job.total_bytes_billed
    );
    db.delete_job(job_ref).await?;
}
# Ok(())
# }
```

- `get_job(&job_ref)` reads a job: its type, state, error, user, labels, statement type, the
  creation, start and end times, and the bytes processed and billed;
- `cancel_job(&job_ref)` asks BigQuery to cancel a running job and returns once the request is
  accepted, before the job stops. Dropping a query's stream or future does not cancel its job;
- `delete_job(&job_ref)` deletes the metadata of a finished job, for example a failed query's SQL
  that should not stay in the job history. It does not cancel a running job.

A short query that BigQuery answered without a job names none. `.job_creation_required()` makes
the query always run as a job.

`stream_jobs(..)` lists jobs, newest first, with the filters of `BigQueryListJobsParams`:

```rust,no_run
# use bigquery::*;
use futures::StreamExt;

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let since: BigQueryInstant = "2026-10-01T00:00:00Z".parse().expect("valid instant");
let mut jobs = db
    .stream_jobs(
        BigQueryListJobsParams::new()
            .with_min_creation_time(since)
            .with_states(vec![BigQueryJobState::Done]),
    )
    .await?;
while let Some(job) = jobs.next().await {
    println!("{} {:?}", job.reference.job_id, job.total_bytes_billed);
}
# Ok(())
# }
```

The filters are the project, `all_users` (which needs the Owner role on the project), the minimum
and maximum creation time, the states, the parent job of a script and the page size.
`stream_jobs_with_errors(..)` yields a failed page as the last item, the same as for the other
listings.

The job state, the job type, the statement type and the table type are enums with an
`Other(String)` case for names the library does not know yet, and `as_str()` gives back the name
BigQuery sent for every value.

## Write streams

There is no listing of write streams. The Storage Write API has no list call, so a stream is
reachable only by the name its creator got back.

## Errors and retries

- A missing dataset, table or job is `DataNotFoundError`;
- creating a dataset that exists, and an update that lost the `if-match` precondition, are
  `DataConflictError`;
- every call goes through the client's retries.

A retry after a lost response repeats a call that may already have succeeded. So a create can
report `DataConflictError` for the dataset it created, and a delete `DataNotFoundError` for the
dataset, table or job it deleted.
