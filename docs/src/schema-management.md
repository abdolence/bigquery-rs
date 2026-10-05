# Schema management

Tables are usually created with DDL in the console or a migration script, and changed by hand
when the code needs a new column. The library supports declaring a table's schema and settings
in Rust instead, next to the structs that read and write it, and making the table match with one
explicit call.

It has three parts: a declaration, a read-only `.plan()` and a `.sync()` that applies it.

## Declaring a table

Everything starts from `db.fluent().schema().table(..)`, then a chain of declarations ending in
`.plan()` or `.sync()`:

```rust,no_run
use bigquery::*;

struct Address {
    city: String,
}

struct Order {
    id: i64,
    customer: String,
    total: String,
    shipping: Address,
    tags: Vec<String>,
    placed_at: jiff::Timestamp,
}

const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");

# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let report = db
    .fluent()
    .schema()
    .table(SHOP.table(ORDERS))
    .columns(|columns| {
        columns.fields([
            columns.field(path!(Order::id)).int64().required(),
            columns.field(path!(Order::customer)).string_with_max_length(64),
            columns.field(path!(Order::total)).numeric_with(10, 2).default_value("0"),
            columns.field(path!(Order::shipping)).record(|address| {
                address.fields([address.field(path!(Address::city)).string()])
            }),
            columns.field(path!(Order::tags)).string().repeated(),
            columns.field(path!(Order::placed_at))
                .timestamp()
                .description("When the customer placed the order"),
        ])
    })
    .primary_key([path!(Order::id)])
    .partition_by_day(path!(Order::placed_at))
    .cluster_by([path!(Order::customer)])
    .description("Orders")
    .labels([("team", "shop")])
    .sync()
    .await?;
println!("{report}");
# Ok(())
# }
```

A missing table is created with one `InsertTable`. An existing one is changed in place where
BigQuery allows it.

`.columns()` declares the columns in table order. Each column needs one type:

- `int64()`, `float64()`, `bool()`;
- `numeric()`, `numeric_with(precision, scale)`, `bignumeric()`, `bignumeric_with(..)`;
- `string()`, `string_with_max_length(n)`, `bytes()`, `bytes_with_max_length(n)`;
- `date()`, `time()`, `datetime()`, `timestamp()`, `interval()`, `range(element)`;
- `geography()`, `json()`;
- `record(|address| ..)` for a STRUCT, with its fields declared the same way;
- `of_type(BigQueryFieldType)` for any of the above as a value.

A column is NULLABLE until `required()` or `repeated()` says otherwise. `description(..)` and
`default_value(..)` are optional, and `renamed_from(..)` is described [below](#renaming-a-column).

The table settings are `primary_key(..)` (always `NOT ENFORCED`, as every BigQuery key),
`partition_by_hour/day/month/year(column)` or `partition_by(BigQueryPartitioning)`,
`partition_expiration(Duration)`, `cluster_by(..)`, `description(..)`, `labels(..)` and
`expiration(BigQueryInstant)`.

Be aware a default value is trusted SQL. It is sent to BigQuery as it is, and written into
generated DDL as one parenthesized expression, so it must never carry text from your users. A
string default is written as its own quoted literal, `default_value("'none'")`. Descriptions,
labels and option values are always sent as values or escaped literals.

The library checks only what it needs itself to build the requests: a column without a type, an
empty column name or one with `.` or a control character, two columns that differ only in case,
`renamed_from` on a nested field, a partitioning column that is not declared, `cluster_by` or
`primary_key` with no columns, `partition_expiration` without partitioning and `snapshot_first()`
without a recreate opt-in. These fail `.plan()` and `.sync()` with `InvalidParametersError`
before any request. Lengths, naming rules and label rules are left to BigQuery.

## plan() versus sync()

`.plan()` is read-only. It sends one `GetTable` (and `ListRowAccessPolicies` when a recreate is
planned) and reports what `.sync()` would do, writing nothing.

`.sync()` plans the same way and then applies the plan.

Both return a value with a `Display` impl, so it can be printed or logged directly:

```rust,no_run
# use bigquery::*;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let plan = db
    .fluent()
    .schema()
    .table(SHOP.table(ORDERS))
    .columns(|columns| {
        columns.fields([
            columns.field("id").int64().required(),
            columns.field("customer").string(),
            columns.field("note").string(),
        ])
    })
    .plan()
    .await?;

if !plan.is_empty() {
    println!("{plan}");
}
# Ok(())
# }
```

`BigQueryTablePlan` has:

- `create`: the table to create, when it does not exist;
- `changes`: the in-place changes, in the order `.sync()` sends them;
- `withheld`: changes left out until the declaration opts in to them, each with its
  `BigQueryWithheldReason` (`PruneUndeclared` or `AllowWidening`);
- `recreate`: the recreate `.sync()` runs instead of `changes`, when a change is impossible in
  place and a recreate opt-in applies;
- `impossible` and `refusal`: the changes that are impossible in place, and why `.sync()` refuses
  them.

`BigQueryTableSyncReport` from `.sync()` has the same shape for what was done: `created`,
`applied`, `withheld`, `recreated`, `snapshot` and `dropped_data`, the last one listing every
column or row a sync deleted.

## One statement owns one table

A chain names exactly one table, and that table is the unit of ownership: `.prune_undeclared()`
never reaches anything outside it. Several tables need several statements, for example one per
table in a startup function.

## What a declaration leaves out

What the chain declares is what `.sync()` makes the table hold. What it leaves undeclared is kept
as the table has it:

- an undeclared column, label, clustering, primary key or partitioning is kept and listed under
  `withheld` with `PruneUndeclared`;
- an undeclared description, default value or expiration is kept and not reported.

So a declaration can start small, with only the columns your code needs, and the rest of the table
stays untouched.

Before comparing, both sides are normalised the way BigQuery itself compares them. `GetTable`
returns the legacy type names, so `INTEGER` equals INT64, `FLOAT` equals FLOAT64, `RECORD` equals
STRUCT, etc. A column created by DDL comes back with an empty mode, which equals NULLABLE. Type
parameters are part of the type, so `STRING(10)` and `STRING(20)` differ, and `NUMERIC(10)` equals
`NUMERIC(10, 0)`. Column names compare ignoring case.

## Change classes

Every difference between the declaration and the table is one `BigQuerySchemaChange`, and BigQuery
applies each kind in its own way:

| Change | Applied by | Needs |
|---|---|---|
| add a NULLABLE or REPEATED column, a field inside an existing RECORD, a RECORD column | `PatchTable` | |
| add a column with a default value | `PatchTable`, then a second `PatchTable` for the default | |
| relax REQUIRED to NULLABLE | `PatchTable` | |
| set a column default, a column or table description | `PatchTable` | |
| add or change a label, the expiration, the partition expiration | `PatchTable` | |
| add or change clustering, add or change the primary key | `PatchTable` | |
| remove the primary key | `PatchTable` | `prune_undeclared()` |
| remove a label or the clustering | `UpdateTable` | `prune_undeclared()` |
| rename a column | DDL `RENAME COLUMN` | `renamed_from(..)` |
| widen a column type | DDL `ALTER COLUMN SET DATA TYPE` | `allow_widening()` |
| drop a column, with every value in it | DDL `DROP COLUMN` | `prune_undeclared()` |
| anything else | impossible in place | a recreate opt-in |

BigQuery cannot add a column with a default in one step, so the column is added first and the
default set right after. Rows already in the table stay NULL in that column; only new rows get the
default. The same holds for a default set on an existing column.

Impossible in place means BigQuery has no call or statement that does it without recreating the
table:

- a type change that is not a widening, INT64 to FLOAT64 and every narrowing included;
- a change between a RECORD and another type, or a widening of a nested field;
- NULLABLE to REQUIRED, and any change to or from REPEATED;
- a new REQUIRED column or nested field;
- dropping a nested field;
- adding or changing the partitioning, or removing it with `prune_undeclared()`.

Without a recreate opt-in `.sync()` refuses such a plan and writes nothing, not even the changes
that are possible in place. The error is `SchemaChangeRefused`, carrying the plan:

```rust,no_run
# use bigquery::*;
# use bigquery::errors::BigQueryError;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let synced = db
    .fluent()
    .schema()
    .table(SHOP.table(ORDERS))
    .columns(|columns| columns.fields([columns.field("id").string().required()]))
    .sync()
    .await;

match synced {
    Ok(report) => println!("{report}"),
    Err(BigQueryError::SchemaChangeRefused(refused)) => {
        for change in &refused.plan.impossible {
            eprintln!("impossible in place: {change}");
        }
    }
    Err(err) => return Err(err),
}
# Ok(())
# }
```

## The order sync() writes changes in

`.sync()` reads the table once and writes in a fixed order:

1. one `PatchTable` with every change that can go into a patch;
2. a second `PatchTable` with the default values of the columns the first one added;
3. one `UpdateTable` for removed labels and clustering, since a patch cannot remove them;
4. DDL through a query, one statement at a time: renames, then widenings, then drops.

Patch and Update carry the etag of the table the sync read, as an `if-match` precondition. When the
table changed in between, the write fails with `DataConflictError` and nothing after it is sent.
Run the sync again: it reads the table as it is now. Changes sent before a failure stay applied, and
the sync logs at `warn` what it applied so far.

BigQuery limits how often one table can be updated: about 5 DDL statements or 7 to 8 patches in a
quick burst, then it starts rejecting them. So a sync sends its first five writes back to back and
spaces the rest about 2 seconds apart, and the rate-limit errors are retried.

## Rolling out a change

BigQuery applies a schema change at once, but running writers and readers see it later. During a
rolling update old and new replicas run side by side, so a sync that removes a column before the
old replicas stop is the same as removing it under them.

So run `.sync()` once per release, in two steps:

- **before the deploy**, the sync without `.prune_undeclared()`: it adds columns, relaxes REQUIRED,
  sets descriptions, labels, clustering, etc., all of which keep running writers working;
- **after the rollout has finished**, the same sync with `.prune_undeclared()`, which drops what
  the release no longer declares.

```rust,no_run
# use bigquery::*;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
fn orders(db: &BigQueryDb) -> BigQueryTableSchemaBuilder<'_> {
    db.fluent()
        .schema()
        .table(SHOP.table(ORDERS))
        .columns(|columns| {
            columns.fields([
                columns.field("id").int64().required(),
                columns.field("customer").string(),
                columns.field("channel").string(),
            ])
        })
}

# async fn example(db: BigQueryDb, rollout_finished: bool) -> BigQueryResult<()> {
let report = if rollout_finished {
    orders(&db).prune_undeclared().sync().await?
} else {
    orders(&db).sync().await?
};
println!("{report}");
# Ok(())
# }
```

From a Kubernetes `Job` or a CI/CD step, run the first one before the deploy step and the second one
once the new version is serving.

What writers and readers see after a sync:

- **New columns.** New writer connections accept rows with the new column about a second after the
  sync. Open connections get BigQuery's `updated_schema` after about 7 seconds, and the library's
  streaming writer encodes the next batches against it. Nothing is lost on the way.
- **Relaxed columns.** An open connection can keep rejecting NULL for a relaxed column after the
  sync: for 2.1 s in one test and still after 5 minutes in another. A fresh connection accepted
  NULL after 0.4 s and 11.5 s. The library's streaming writer reconnects by itself once it sees
  the relaxed column, so you only need to reconnect other writers before they send NULL.
- **Dropped columns.** For about 9 seconds after a drop, an open writer's values for the column are
  accepted and lost silently. Then the connection fails with `Input schema has more fields than
  BigQuery schema`. That is why a drop waits for the second step.
- **Readers.** A read with selected fields that names a new or renamed column fails with `The
  following selected fields do not exist in the table schema` for about 30 seconds after the sync.
  A read without selected fields sees the change at once.

## Renaming a column

`renamed_from(old_name)` declares that a column used to be called `old_name`:

```rust,no_run
# use bigquery::*;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
db.fluent()
    .schema()
    .table(SHOP.table(ORDERS))
    .columns(|columns| {
        columns.fields([
            columns.field("id").int64().required(),
            columns.field("customer_name").string().renamed_from("customer"),
        ])
    })
    .sync()
    .await?;
# Ok(())
# }
```

When the table has `customer` and no `customer_name`, `.sync()` renames it with `RENAME COLUMN` and
the values are kept. Once the table has `customer_name`, the declaration is a no-op, so it can stay
in the code for a while.

For writers a rename is a drop of the old name: a writer still sending `customer` loses that value
silently for a few seconds and then fails. `renamed_from(..)` is not held back by
`.prune_undeclared()`, so the rename runs in whichever sync carries it. Be aware not to add
`renamed_from(..)` to the declaration the first step runs: add it only to the one used after the
rollout, once the writers have moved to the new name. Only top-level columns can be renamed.

Full example available [here](https://github.com/abdolence/bigquery-rs/blob/master/examples/schema-sync.rs).

## Widening a column

A declared type that is wider than the column's is withheld with `AllowWidening` until
`.allow_widening()` is set. Then `.sync()` widens it with `ALTER COLUMN SET DATA TYPE`, and the
values are kept. The library takes these pairs as widenings:

- INT64 to NUMERIC or BIGNUMERIC, and NUMERIC to BIGNUMERIC, without parameters;
- `NUMERIC(p, s)` or `BIGNUMERIC(p, s)` to parameters that fit every value of the old ones, or to
  no parameters;
- `STRING(n)` and `BYTES(n)` to a longer maximum length, or to no maximum.

INT64 to NUMERIC and a longer `STRING(n)` were checked against BigQuery; the other pairs follow
GoogleSQL's assignability rules. INT64 to FLOAT64 is not a widening for BigQuery, and every other
type change is impossible in place. Only top-level columns can be widened.

## Removing what you no longer declare

`.prune_undeclared()` makes `.sync()` also remove what the table has and the statement does not
declare:

- undeclared columns are dropped, with every value in them;
- undeclared labels, clustering and the primary key are removed.

Be aware this is the one in-place change that loses data. Every dropped column is logged at `warn`
and listed in the report's `dropped_data`. An undeclared nested field or partitioning cannot be
removed in place, so with `.prune_undeclared()` set they make the plan need a recreate.

## Recreating a table

When a change is impossible in place, the only way is to drop the table and create it again, losing
its rows. No data is migrated. A declaration opts in to that with one of:

- `recreate_if_empty()`: recreate only when `GetTable` reports `num_rows == 0`, otherwise refuse
  with `BigQueryRefusal::NotEmpty`;
- `dangerously_recreate_with_data_loss()`: recreate whatever the table holds.

The last of the two in a chain wins. `snapshot_first()` adds a `CREATE SNAPSHOT TABLE` before the
recreate, as a cheap undo:

```rust,no_run
# use bigquery::*;
# const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
# const EVENTS: BigQueryTableId = BigQueryTableId::from_static("events");
# async fn example(db: BigQueryDb) -> BigQueryResult<()> {
let report = db
    .fluent()
    .schema()
    .table(SHOP.table(EVENTS))
    .columns(|columns| {
        columns.fields([
            columns.field("id").string().required(),
            columns.field("happened_at").timestamp(),
        ])
    })
    .partition_by_month("happened_at")
    .dangerously_recreate_with_data_loss()
    .snapshot_first()
    .sync()
    .await?;

if let Some(snapshot) = &report.snapshot {
    println!("snapshot taken: {snapshot}");
}
# Ok(())
# }
```

The table is recreated as declared, plus whatever it has undeclared and not pruned. There are two
ways, and the library picks one by what changes:

| | `CREATE OR REPLACE TABLE` | `DROP TABLE` and `CREATE TABLE` in one script |
|---|---|---|
| used when | the partitioning and clustering stay the same | the partitioning or clustering changes, which `CREATE OR REPLACE` cannot do |
| rows | lost | lost |
| columns, key, partitioning, clustering, description, labels, expirations | as declared or kept | as declared or kept |
| table IAM bindings | kept | lost |
| row access policies | lost | lost |

The plan lists the row access policies that will be lost, and `BigQueryRecreate` names the method.
The snapshot is named `<table>_snapshot_<unix seconds>` in the same dataset, and the library never
deletes it.

These are the limits of a recreate, as measured against BigQuery, and the library does not hide
any of them:

- **`num_rows` lags Storage Write.** Rows written through committed or pending streams show in
  `num_rows` after 60 to 70 seconds, and rows on the default stream can take more than 5 minutes.
  So with `recreate_if_empty()` a table written seconds ago can read as empty and be recreated,
  its rows lost. The row and byte counts in the plan and in `dropped_data` have the same lag and
  can be lower than what is lost. Use `recreate_if_empty()` for tables that are new or being
  experimented on, with no writer running.
- **The snapshot misses the streaming buffer.** A `CREATE SNAPSHOT TABLE` does not hold rows still
  in the streaming buffer, which is every row written through Storage Write in roughly the last
  minute or more.
- **Nothing guards a recreate.** BigQuery takes no precondition on `DROP TABLE` or
  `CREATE OR REPLACE`, so a change made to the table between the plan and the recreate is lost too.
- **Writers do not see it.** A writer connection opened before the recreate keeps getting
  successful acks for about 4 to 7 seconds, for rows that are lost. Then the next append waits about
  120 seconds for `DeadlineExceeded`. New writers can get `NotFound` (`is truncated`, `is
  re-created`) for up to a few minutes, so the report does not promise that the table is writable
  yet. A writer still using the old column types gets `InvalidArgument` at once, and that one is not
  retryable.
- **Readers do not see it either.** A read session opened before the recreate keeps returning the
  old rows and the old schema. Readers have to start a new session.

So stop writers and readers before a recreate. A dangerous recreate is logged at `warn` with the row
count it read.

Full example available [here](https://github.com/abdolence/bigquery-rs/blob/master/examples/recreate-table.rs).

## Logs and spans

Every `.plan()` and `.sync()` runs in a `BigQuery schema` span with the table in the
`/bigquery/table` field. A created table and every applied write are logged at `info`, and
the data-losing steps at `warn`: a dropped column, a dangerous recreate and a sync that failed part
way, with the report of what it had applied.
