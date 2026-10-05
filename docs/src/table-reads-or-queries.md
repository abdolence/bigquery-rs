# Table reads or queries

The library reads data in two ways:

- a [table read](./reading-tables.md), `select().from(..)` with `.filter(..)`, goes through the
  Storage Read API. It runs no query and no job;
- a [query](./queries.md), `query(..)` with named parameters, runs GoogleSQL through the v2
  `Query` call.

Both decode rows with the same Arrow decoder, so a type maps the same way in each, see
[Type mapping](./types.md).

The same rows both ways:

```rust,no_run
# use bigquery::*;
# use serde::Deserialize;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const PEOPLE: BigQueryTableId = BigQueryTableId::from_static("people");
#[derive(Debug, Deserialize)]
struct Person {
    name: String,
    city: String,
}

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let read: Vec<Person> = db
    .fluent()
    .select()
    .from(SHOP.table(PEOPLE))
    .filter(|filter| filter.field(path!(Person::city)).eq("Malmö"))
    .obj()
    .query()
    .await?;

let queried: Vec<Person> = db
    .fluent()
    .query("SELECT name, city FROM shop.people WHERE city = @city")
    .param("city", "Malmö")
    .obj::<Person>()
    .query()
    .await?;
# let _ = (read, queried);
# Ok(())
# }
```

## Comparison

| | Table read | Query |
|---|---|---|
| What it reads | One table: the columns you select, the rows the filter keeps, at a snapshot time or as a sample | Any GoogleSQL: joins, aggregates, `ORDER BY`, `LIMIT`, views, computed columns |
| Writes | No | DML and DDL |
| Values in conditions | Escaped literals in the session's `row_restriction`, Storage Read has no parameters | Query parameters, the value never becomes SQL text |
| Job | None, so no job stats, dry run or cancellation | Short query mode answers small ones without a job, others get one |
| Columns read | The ones your structure names, by itself | The ones the `SELECT` names |
| First row of a small result | After opening a read session | Inline in the first response, about 0.1 s in the [benchmarks](./benchmarks.md#small-query-latency) |
| Large results | Several streams in parallel, each resumed at its row offset after a retryable error | Read from the job's destination table, through the same Storage Read path |
| Billing | Storage Read: bytes read | Query: bytes processed, then nothing for reading the result |

Be aware that Storage Read cannot read views, logical or materialized, and external tables, as
the [Storage Read API limitations](https://cloud.google.com/bigquery/docs/reference/storage#limitations)
say. Query them instead.

## Billing hints

Prices here are the on-demand list prices in USD from
[BigQuery pricing](https://cloud.google.com/bigquery/pricing) in October 2026, so check them for
your region and contract:

- a table read is billed $1.10 per TiB read, and the first 300 TiB a month for each billing
  account are free. Reads within the same location are free of data transfer;
- a query is billed $6.25 per TiB processed, with the first 1 TiB a month free. It bills every
  column the query touches over the whole table, unless partitioning or clustering prune it,
  even with `LIMIT`, and at least 10 MB per table referenced;
- reading a query result is free: the result is a temporary table, and reads of those are not
  billed. Neither is a query answered from the query cache;
- a table read bills the columns it reads, so select only the ones you need. The automatic
  projection from your structure does it for you, see
  [Selecting columns](./reading-tables.md#selecting-columns).

So for a scan of a large table a table read is probably much cheaper, a bit more than a sixth of
the query price per byte, and free under the monthly allowance. In the
[benchmarks](./benchmarks.md#cost) the 215 MB table scans went through the Storage Read free
tier, while each `SELECT *` scan of the official crate billed the table. I didn't measure whether a row
filter reduces the bytes billed for a table read, so I would not count on it.

`.maximum_bytes_billed(..)` makes a query fail without running if it would bill more, and a
[dry run](./queries.md#dry-run) tells you the bytes before you run it.

## Which one to use

- **Rows of one table, filtered by values**: a table read. Exports, syncs, full scans, "the
  orders of this customer". It needs no job, reads in parallel and resumes by itself;
- **Joins, aggregates, ordering, `LIMIT`, views or computed columns**: a query, with named
  parameters for every value;
- **A small lookup, a few rows by key**: probably a query. Short query mode returns it in one
  call, while a table read opens a session first;
- **DML, DDL, or when you need job stats, labels on the job or a dry run**: a query;
- **Arrow for your own processing**: either, both have `record_batches()`.

Be aware not to splice values into SQL text in either path. Use `.filter(..)` instead of
`.filter_sql(..)`, and `.param(..)` instead of `format!` in the query text.
