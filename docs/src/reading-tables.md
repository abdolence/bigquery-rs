# Reading tables

The library reads tables through the BigQuery Storage Read API. A read runs no query and no job:
BigQuery streams the table's columns as Arrow, and the library decodes them into your structures
with serde. When to read a table and when to query it, see
[Table reads or queries](./table-reads-or-queries.md).

```rust,no_run
# use bigquery::*;
# use futures::TryStreamExt;
# use futures::stream::BoxStream;
# use serde::Deserialize;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const PEOPLE: BigQueryTableId = BigQueryTableId::from_static("people");
#[derive(Debug, Deserialize)]
struct Person {
    name: String,
    city: String,
    year: i64,
}

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let people: BoxStream<BigQueryResult<Person>> = db
    .fluent()
    .select()
    .fields(paths!(Person::{name, city, year})) // Optionally select the columns needed
    .from(SHOP.table(PEOPLE))
    .filter(|filter| {
        filter.for_all([
            filter.field(path!(Person::city)).eq("Stockholm"),
            filter.field(path!(Person::year)).ge(2010),
        ])
    })
    .obj() // Reading rows as structures using serde
    .stream_query_with_errors()
    .await?;

let as_vec: Vec<Person> = people.try_collect().await?;
println!("{as_vec:?}");
# Ok(())
# }
```

A read needs the `bigquery.readsessions.create` permission on the project and read access to the
table, the same as any Storage Read client.

## Selecting columns

`.fields(..)` takes the column names to read. `path!` and `paths!` build them from the fields of
your structure, so a renamed field is a compile error instead of a read that fails on a column the
table does not have:

- `path!(Person::city)` is `"city"`;
- `paths!(Person::{name, city})` is `vec!["name", "city"]`;
- `path!(Person::home.county)` is `"home.county"`, a subfield of a STRUCT column.

Without `.fields(..)`, a typed read selects by itself the columns that the top-level fields of
your structure name, so it does not read columns it would throw away. It costs one `GetTable` call
to learn the table's columns. A structure with `#[serde(flatten)]`, a map, `serde_json::Value` or
a tuple has no complete list of fields, so it reads every column. `record_batches()` without
`.fields(..)` reads every column too.

The paths are Rust field names. A field under `#[serde(rename = "...")]` needs
`path_camel_case!` for camelCase columns, or the column name as a plain string.

Be aware that BigQuery checks `selected_fields` against a schema that lags behind: a column
added or renamed in the last 30 seconds or so is refused, and the read fails with
`SchemaMismatchError`. The automatic projection falls back to reading every column in that case
and logs a warning.

## Filtering

`.filter(..)` takes a closure that receives a `BigQueryFilterBuilder` and returns an
`Option<BigQueryFilter>`:

- `f.field(..)` with `eq`, `neq`, `lt`, `le`, `gt`, `ge`, `is_null`, `is_not_null`, `is_in` and
  `is_not_in`;
- `f.for_all` for AND conditions;
- `f.for_any` for OR conditions;
- `f.not` for NOT.

You can nest them, and a `None` entry is dropped, so optional conditions can be written inline:

```rust,no_run
# use bigquery::*;
# use serde::Deserialize;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const PEOPLE: BigQueryTableId = BigQueryTableId::from_static("people");
# #[derive(Deserialize)]
# struct Person {
#     name: String,
#     city: String,
#     year: i64,
# }
# async fn example(db: BigQueryDb, min_year: Option<i64>) -> BigQueryResult<()> {
let people: Vec<Person> = db
    .fluent()
    .select()
    .from(SHOP.table(PEOPLE))
    .filter(|filter| {
        filter.for_all([
            filter.for_any([
                filter.field(path!(Person::city)).is_in(["Malmö", "Lund"]),
                filter.field(path!(Person::city)).is_null(),
            ]),
            min_year.and_then(|year| filter.field(path!(Person::year)).ge(year)),
        ])
    })
    .obj()
    .query()
    .await?;
# let _ = people;
# Ok(())
# }
```

Storage Read has no query parameters: the filter is SQL text in the session's `row_restriction`.
The builder writes values into it as escaped GoogleSQL literals and column names as quoted
identifiers, so a value cannot change the condition it is in, whatever it holds. Values are any
`Serialize`, with the same mapping as query parameters: a string is a STRING, an integer an
INT64, `BigQueryTimestamp` a TIMESTAMP, etc. A condition can name any column, selected or not.

A few things to know:

- a NULL value is refused, since `n = NULL` matches no row; use `is_null()` instead;
- GoogleSQL's three-valued logic applies: a row where the column is NULL matches neither
  `neq(..)` nor `not(eq(..))`;
- a closure that returns `None` reads every row;
- a value without a literal form fails the read before any request, with `SerializeError`;
- the whole restriction is at most 1 MB, a limit BigQuery checks.

Full example available [here](https://github.com/abdolence/bigquery-rs/blob/master/examples/select-table.rs).

### Raw SQL filters

`.filter_sql(..)` sends condition text as it is, for example `"year > 2010 AND city != ''"`.

Be aware not to build this text from user input. It is SQL, and a value spliced into it can
rewrite the condition. Use `.filter(..)` for every condition that carries a value, and keep
`.filter_sql(..)` for fixed text from your own code. `.filter(..)` and `.filter_sql(..)` set the
same restriction, so the last call wins.

## Snapshots and sampling

`.snapshot_time(..)` reads the table as it was at that time instead of now, using BigQuery's time
travel. It works only within the time travel window of the dataset, 7 days by default:

```rust,no_run
# use bigquery::*;
# use serde::Deserialize;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const PEOPLE: BigQueryTableId = BigQueryTableId::from_static("people");
# #[derive(Deserialize)]
# struct Person {
#     name: String,
# }
# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let an_hour_ago = BigQueryInstant::now() - jiff::SignedDuration::from_hours(1);

let people: Vec<Person> = db
    .fluent()
    .select()
    .from(SHOP.table(PEOPLE))
    .snapshot_time(an_hour_ago)
    .obj()
    .query()
    .await?;
# let _ = people;
# Ok(())
# }
```

`.sample_percentage(..)` reads a random sample of about that percentage of the table, above 0 and
up to 100. BigQuery samples by storage blocks, so treat the percentage as approximate, especially
on small tables.

## Reading rows

`.obj::<T>()` decodes the rows into `T` with the library's own Arrow decoder, which covers every
BigQuery type. Then pick one of:

- `query()` to collect every row into a `Vec<T>`, failing on the first error;
- `stream_query_with_errors()` to stream the rows, with every failure as an `Err` item. A row that
  fails to decode is one `Err(DeserializeError)` and the stream goes on;
- `stream_query()` to stream only the rows that decode. Failures are logged at `error!` and
  skipped.

Be aware that a stream from `stream_query()` that ends does not mean every row was read: a read
stream that failed for good ends it too, only with a log line. Use `stream_query_with_errors()`
when you need to know.

Rows are decoded on the task that reads their stream, so `T` is `Send + 'static` and cannot borrow
from the batch.

## Record batches

`.record_batches()` streams the Arrow `RecordBatch`es as BigQuery sent them, after the IPC decode
and decompression. It is a bit faster than typed rows and the way to hand the data to other Arrow
tools. `arrow_array` and `arrow_schema` are re-exported, so you use the same Arrow version as the
library.

`BigQueryBatchRows` decodes a batch into typed rows later, with the same mapping as `.obj()`.
A row that fails is one `Err` item, and the rows after it still decode:

```rust,no_run
# use bigquery::*;
# use futures::TryStreamExt;
# use serde::Deserialize;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const PEOPLE: BigQueryTableId = BigQueryTableId::from_static("people");
# #[derive(Deserialize)]
# struct Person {
#     name: String,
#     year: i64,
# }
# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let mut batches = db
    .fluent()
    .select()
    .fields(paths!(Person::{name, year}))
    .from(SHOP.table(PEOPLE))
    .record_batches()
    .await?;

while let Some(batch) = batches.try_next().await? {
    println!("{} rows, {} columns", batch.num_rows(), batch.num_columns());
    for person in BigQueryBatchRows::<Person>::new(&batch) {
        let person = person?;
        println!("{} {}", person.name, person.year);
    }
}
# Ok(())
# }
```

`BigQueryBatchRows` is not `Send`, so decode a batch where you hold it and do not keep the iterator
across an `.await`.

Full example available [here](https://github.com/abdolence/bigquery-rs/blob/master/examples/record-batches.rs).

## Parallel streams and resume

One read opens one read session, and BigQuery splits the table into several read streams. Each
stream runs on its own task: `ReadRows`, the Arrow decode and, for typed reads, the row decode.
The streams meet in one bounded channel, so a slow consumer slows the read down instead of
buffering the table in memory, and rows from different streams arrive in no particular order.

`BigQueryReadOptions` sets how the session opens:

- `max_stream_count`: the most streams to ask for, the machine's available parallelism by
  default;
- `preferred_min_stream_count`: the fewest streams BigQuery should aim for;
- `compression`: `Lz4` by default, `Zstd` or `None`.

```rust,no_run
# use bigquery::*;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const PEOPLE: BigQueryTableId = BigQueryTableId::from_static("people");
# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let batches = db
    .fluent()
    .select()
    .from(SHOP.table(PEOPLE))
    .options(
        BigQueryReadOptions::new()
            .with_max_stream_count(8)
            .with_compression(BigQueryReadCompression::Zstd),
    )
    .record_batches()
    .await?;
# let _ = batches;
# Ok(())
# }
```

BigQuery decides the real count and often gives fewer streams than asked for. In the
[benchmarks](./benchmarks.md) it gave 4 streams of the 16 asked for on a 1M-row table, and 1 for a
200,000-row query result.

A stream that fails with a retryable error is resumed at its row offset after a backoff, so the
rows it already sent are not read again. Consecutive failures are capped by the client's `max_retries`. A
stream that fails for good is one `Err` item on the read, and then the whole read ends, since a
scan that lost a stream is incomplete.

One case is never retried: an INTERVAL whose time part does not fit Arrow's nanoseconds fails the
stream on BigQuery's side, and every resume would fail the same way. Read such a column through a
query with `CAST(.. AS STRING)` instead.

The read session, its stream count, BigQuery's estimate of the bytes scanned and the rows and bytes
read are recorded on the `BigQuery Read` tracing span.
