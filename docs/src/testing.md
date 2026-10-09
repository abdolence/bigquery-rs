# Testing with the fake

The library provides a fake BigQuery for your tests, behind the `testing` feature.
`BigQueryFake` runs a gRPC server on a loopback port and hands out a real `BigQueryDb` connected
to it, so the code under test runs unchanged, through the same requests, codecs, retries and
errors as against BigQuery. It needs no credentials and no network.

A test scripts the fake with your own serde types:

- a query rule answers the statement it matches with rows, DML counts or a failure;
- a table holds rows, which reads serve and writes append to, and which the test reads back;
- a fault makes the calls of one RPC fail.

Enable the feature in your dev-dependencies, next to an async test runtime:

```toml
[dev-dependencies]
bigquery = { version = "0.8", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

The examples below share a row type and the code under test, and each of them is the body of a
`#[tokio::test] async fn ...() -> BigQueryResult<()>`:

```rust
use bigquery::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Order {
    id: i64,
    customer: String,
    total: f64,
}

const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
const ORDERS_OF: &str = "SELECT id, customer, total FROM shop.orders WHERE customer = @customer";

async fn orders_of(db: &BigQueryDb, customer: &str) -> BigQueryResult<Vec<Order>> {
    db.fluent().query(ORDERS_OF).param("customer", customer).obj().query().await
}

async fn save(db: &BigQueryDb, orders: &[Order]) -> BigQueryResult<BigQueryWriteSummary> {
    db.fluent().insert().into(SHOP.table(ORDERS)).objects(orders).execute().await
}
```

## Queries

There is no SQL engine in the fake. A query answers only as a rule scripts it:

```rust
# use bigquery::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
# struct Order { id: i64, customer: String, total: f64 }
# const ORDERS_OF: &str = "SELECT id, customer, total FROM shop.orders WHERE customer = @customer";
# async fn orders_of(db: &BigQueryDb, customer: &str) -> BigQueryResult<Vec<Order>> {
#     db.fluent().query(ORDERS_OF).param("customer", customer).obj().query().await
# }
# #[tokio::main(flavor = "current_thread")]
# async fn main() -> BigQueryResult<()> {
use bigquery::testing::BigQueryFake;

let fake = BigQueryFake::start().await?;
let alice = vec![Order { id: 1, customer: "Alice".into(), total: 120.0 }];
let rule = fake
    .query(ORDERS_OF)
    .param("customer", "Alice")
    .returns_rows(|columns| columns.from_type::<Order>(), &alice)?;

assert_eq!(orders_of(fake.db(), "Alice").await?, alice);
assert_eq!(rule.calls(), 1);
# Ok(())
# }
```

The rows are any `Serialize` type, and their columns are declared as a table schema declares them,
here with `|columns| columns.from_type::<Order>()`. A rule matches:

- the SQL text exactly, with no whitespace or case folding. `query_matching(|sql| ...)` takes a
  predicate over the text for looser matching;
- a statement from `sql_file!` by the text of its file;
- any parameters, unless the rule declares some with `.param(..)`, `.params(..)` or
  `.positional_param(..)`. Then the call's named parameters must equal them as a set, and its
  positional ones in order.

Rules are tried in the order they were registered, and the first that matches answers.
`.times(n)` limits a rule to `n` calls, retries included, so a later rule answers the calls after
them. Besides `returns_rows`, a rule answers with `returns_dml` for a DML statement and the rows it
changed, `returns_statement` for DDL, `fails` for a refused call and `fails_job` for a job that
fails once it runs. One rule answers every terminal of a call: the rows for `query()`, the stats
for `execute()` and the schema for `dry_run()`, with `.bytes_processed(..)` if set.

## Tables, reads and writes

A table on the fake holds rows. `fake.table(..)` creates it with its columns, and its dataset if
that is missing, and `.rows(..)` adds rows a read is served:

```rust
# use bigquery::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
# struct Order { id: i64, customer: String, total: f64 }
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
# #[tokio::main(flavor = "current_thread")]
# async fn main() -> BigQueryResult<()> {
use bigquery::testing::BigQueryFake;

let fake = BigQueryFake::start().await?;
let orders = vec![
    Order { id: 1, customer: "Alice".into(), total: 120.0 },
    Order { id: 2, customer: "Bob".into(), total: 80.5 },
];
fake.table(SHOP.table(ORDERS), |columns| columns.from_type::<Order>())
    .rows(&orders)
    .create()?;

let read: Vec<Order> = fake.db().fluent().select().from(SHOP.table(ORDERS)).obj().query().await?;
assert_eq!(read, orders);
# Ok(())
# }
```

Writes append to the table, and `fake.rows::<T>(table)` reads back the rows written:

```rust
# use bigquery::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
# struct Order { id: i64, customer: String, total: f64 }
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
# async fn save(db: &BigQueryDb, orders: &[Order]) -> BigQueryResult<BigQueryWriteSummary> {
#     db.fluent().insert().into(SHOP.table(ORDERS)).objects(orders).execute().await
# }
# #[tokio::main(flavor = "current_thread")]
# async fn main() -> BigQueryResult<()> {
use bigquery::testing::BigQueryFake;

let fake = BigQueryFake::start().await?;
fake.table(SHOP.table(ORDERS), |columns| columns.from_type::<Order>())
    .create()?;
let orders = vec![
    Order { id: 1, customer: "Alice".into(), total: 120.0 },
    Order { id: 2, customer: "Bob".into(), total: 80.5 },
];

let summary = save(fake.db(), &orders).await?;

assert_eq!(summary.rows_written, 2);
let written: Vec<Order> = fake.rows(SHOP.table(ORDERS))?;
assert_eq!(written, orders);
# Ok(())
# }
```

`rows` returns the visible rows, in the order they were acknowledged. Rows become visible as
BigQuery makes them visible for each write mode:

- the default and committed streams: when acknowledged;
- a buffered stream: up to the last flushed offset;
- a pending stream: at its commit, and never without one.

A table read is served the table's rows, projected to the selected columns. The fake evaluates
no filter, so a read with a row restriction needs a rule, `fake.read(table).row_restriction(..)`,
with the rows it returns. `fake.reject_rows::<T>(table, reason, |row| ...)` fails each append
request that has a row the predicate accepts, with a row error per such row, and writes nothing
of that request, as BigQuery does.

A CDC writer's rows are recorded as changes and not applied to the table's rows.
`fake.changes::<T>(table)` returns them, each with its change type and sequence number.

Datasets and tables, from `fake.create_dataset(..)`, `fake.table(..)` or the code under test, are
the fake's state, and the admin calls are served from it.

## Faults and retries

A query rule's `fails` scripts the failure of a query. `fake.fault(rpc)` does the same for every
other RPC, and `.on_table(..)` narrows it to the calls on one table. A fault answers before every
rule and before the tables, as one of:

- `BigQueryFakeFault::status(code, message)`, a gRPC status, which the client maps as it maps
  BigQuery's own: `NotFound` is a `DataNotFoundError`, `Unavailable` a retryable
  `DatabaseError`, etc.;
- `BigQueryFakeFault::ConnectionDropped`, a connection closed without an answer, which the
  client retries as a transport error;
- `BigQueryFakeFault::Hang`, a call that never answers. The client waits until its own timeout,
  if it has one.

Retries against the fake wait no backoff, so a test that scripts failures runs at full speed.
A failure a retry gets past, then an outage the client gives up on:

```rust
# use bigquery::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
# struct Order { id: i64, customer: String, total: f64 }
# const ORDERS_OF: &str = "SELECT id, customer, total FROM shop.orders WHERE customer = @customer";
# async fn orders_of(db: &BigQueryDb, customer: &str) -> BigQueryResult<Vec<Order>> {
#     db.fluent().query(ORDERS_OF).param("customer", customer).obj().query().await
# }
# #[tokio::main(flavor = "current_thread")]
# async fn main() -> BigQueryResult<()> {
use bigquery::testing::{BigQueryFake, BigQueryFakeCode, BigQueryFakeFault};

let fake = BigQueryFake::start().await?;
let alice = vec![Order { id: 1, customer: "Alice".into(), total: 120.0 }];
let unavailable =
    || BigQueryFakeFault::status(BigQueryFakeCode::Unavailable, "backend went away");
let lost = fake.query(ORDERS_OF).times(1).fails(unavailable())?;
let answer = fake
    .query(ORDERS_OF)
    .param("customer", "Alice")
    .returns_rows(|columns| columns.from_type::<Order>(), &alice)?;
let outage = fake
    .query(ORDERS_OF)
    .param("customer", "Bob")
    .fails(unavailable())?;

assert_eq!(orders_of(fake.db(), "Alice").await?, alice);
assert_eq!((lost.calls(), answer.calls()), (1, 1));

let failed = orders_of(fake.db(), "Bob").await;
assert!(matches!(&failed, Err(err) if err.retry_possible()), "{failed:?}");
assert_eq!(outage.calls(), 4, "the first attempt and max_retries = 3 retries");
# Ok(())
# }
```

On `AppendRows`, `ConnectionDropped` writes the request and then drops the connection, as an
acknowledgement lost in transit. The client sends the request again, so on the default stream,
which is at least once, its rows are written twice. A write with `.exactly_once()` sends it with
its offset, and the table keeps one copy:

```rust
# use bigquery::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
# struct Order { id: i64, customer: String, total: f64 }
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
# #[tokio::main(flavor = "current_thread")]
# async fn main() -> BigQueryResult<()> {
use bigquery::testing::{BigQueryFake, BigQueryFakeFault, BigQueryFakeRpc};

let fake = BigQueryFake::start().await?;
fake.table(SHOP.table(ORDERS), |columns| columns.from_type::<Order>())
    .create()?;
let lost = fake
    .fault(BigQueryFakeRpc::AppendRows)
    .times(1)
    .respond(BigQueryFakeFault::ConnectionDropped)?;
let orders = vec![Order { id: 1, customer: "Alice".into(), total: 120.0 }];

fake.db()
    .fluent()
    .insert()
    .into(SHOP.table(ORDERS))
    .objects(&orders)
    .exactly_once()
    .execute()
    .await?;

assert_eq!(fake.rows::<Order>(SHOP.table(ORDERS))?, orders);
assert_eq!(lost.calls(), 1);
# Ok(())
# }
```

## Unmatched calls

A call that nothing answers, such as a query no rule matches or an RPC the fake does not serve,
fails with `Unimplemented`, which the client does not retry. The fake records it, and
`fake.verify()` panics with every such call and the rules that were there to answer it. It also
panics for a rule limited with `.times(n)` that did not answer exactly `n` calls.

`verify` runs when the fake is dropped, so a test fails on an unscripted call even when the code
under test swallows its error:

```rust,should_panic
# use bigquery::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
# struct Order { id: i64, customer: String, total: f64 }
# const ORDERS_OF: &str = "SELECT id, customer, total FROM shop.orders WHERE customer = @customer";
# async fn orders_of(db: &BigQueryDb, customer: &str) -> BigQueryResult<Vec<Order>> {
#     db.fluent().query(ORDERS_OF).param("customer", customer).obj().query().await
# }
# #[tokio::main(flavor = "current_thread")]
# async fn main() -> BigQueryResult<()> {
use bigquery::testing::BigQueryFake;

let fake = BigQueryFake::start().await?;
fake.query(ORDERS_OF)
    .param("customer", "Alice")
    .returns_rows(|columns| columns.from_type::<Order>(), Vec::<Order>::new())?;

let unmatched = orders_of(fake.db(), "Bob").await;
assert!(matches!(&unmatched, Err(err) if !err.retry_possible()), "{unmatched:?}");

drop(fake); // Panics: no rule answers the query for Bob
# Ok(())
# }
```

If the test is already panicking when the fake is dropped, the problems are logged with
`tracing::error!` instead, so the first panic is the one the test reports.

## Choosing the fake or BigQuery at startup

`fake.db()` is a real `BigQueryDb` connected to the fake's loopback server, so the fake needs no
trait shared with the real client. Code that takes a `BigQueryDb`, as an argument or a field of
the application state, runs unchanged against either. This also makes it possible to run the
whole application against the fake, with a factory that picks one at startup from the
application's own configuration, such as an environment variable. The library itself reads no
variable for this.

The application forwards the `testing` feature with a feature of its own, so production builds do
not compile the fake in:

```toml
[dependencies]
bigquery = "0.8"

[features]
fake-bigquery = ["bigquery/testing"]
```

The factory returns an owner of the client. Dropping a `BigQueryFake` stops its server and runs
`verify()`, so the owner keeps the fake alive next to the client it hands out:

```rust,no_run
# #![allow(unexpected_cfgs)]
# use bigquery::*;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
# struct Order { id: i64, customer: String, total: f64 }
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
# const ORDERS_OF: &str = "SELECT id, customer, total FROM shop.orders WHERE customer = @customer";
# async fn orders_of(db: &BigQueryDb, customer: &str) -> BigQueryResult<Vec<Order>> {
#     db.fluent().query(ORDERS_OF).param("customer", customer).obj().query().await
# }
pub struct AppBigQuery {
    db: BigQueryDb,
    #[cfg(feature = "fake-bigquery")]
    _fake: Option<bigquery::testing::BigQueryFake>,
}

impl AppBigQuery {
    pub fn db(&self) -> &BigQueryDb {
        &self.db
    }
}

/// BigQuery in `project`, or a seeded fake when the build has `fake-bigquery` and
/// `APP_BIGQUERY=fake` is set.
pub async fn bigquery_db(project: &str) -> BigQueryResult<AppBigQuery> {
    #[cfg(feature = "fake-bigquery")]
    if std::env::var("APP_BIGQUERY").as_deref() == Ok("fake") {
        let fake = bigquery::testing::BigQueryFake::start().await?;
        seed(&fake)?;
        return Ok(AppBigQuery {
            db: fake.db().clone(),
            _fake: Some(fake),
        });
    }
    Ok(AppBigQuery {
        db: BigQueryDb::new(project).await?,
        #[cfg(feature = "fake-bigquery")]
        _fake: None,
    })
}

#[cfg(feature = "fake-bigquery")]
fn seed(fake: &bigquery::testing::BigQueryFake) -> BigQueryResult<()> {
    let alice = vec![Order { id: 1, customer: "Alice".into(), total: 120.0 }];
    fake.table(SHOP.table(ORDERS), |columns| columns.from_type::<Order>())
        .rows(&alice)
        .create()?;
    fake.query(ORDERS_OF)
        .returns_rows(|columns| columns.from_type::<Order>(), &alice)?;
    Ok(())
}

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> BigQueryResult<()> {
// The rest of the application sees only the client
let bigquery = bigquery_db("my-gcp-project-id").await?;
let orders = orders_of(bigquery.db(), "Alice").await?;
# let _ = orders;
# Ok(())
# }
```

The fake answers a query only from a rule, so the fake branch seeds a rule for every query the
application sends, besides the tables it reads and writes. An application run against the fake
with `APP_BIGQUERY=fake cargo run --features fake-bigquery` panics when the fake is dropped if a
call went unanswered, as a test does.

## Limits

The fake binds its own port, so tests run in parallel, and a fake is `Send + Sync`, so a test can
share it with the tasks it spawns. A call sees the rules registered before it arrives.

Tokio's paused time, `#[tokio::test(start_paused = true)]`, is not supported. A paused runtime
jumps to its next timer whenever it is idle, and waiting on the fake's loopback socket counts as
idle.

The fake does not provide:

- SQL evaluation. The DDL that `.sync()` sends for a rename, drop or widen reaches the query rules
  and does not change the table;
- read filters. A row restriction needs a read rule, and `sample_percentage` and `snapshot_time`
  are served the current rows;
- failures in the middle of a read stream, and partial acknowledgements of an append;
- polling of running jobs, other than for `fails_job` and queries with a destination table;
- CDC changes applied to the table's rows;
- Arrow appends in Arrow types other than the table's own;
- the server side checks of `STRING(n)`, `NUMERIC(p,s)`, default values, request limits and
  quotas;
- paging: a list call returns one page;
- `ListJobs`, `DeleteJob`, models, routines, row access policies, table snapshots and copies.

Full example available [here](https://github.com/abdolence/bigquery-rs/blob/master/examples/testing-fake.rs).
