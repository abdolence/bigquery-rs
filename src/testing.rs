//! A fake BigQuery for your tests, behind the `testing` feature.
//!
//! [`BigQueryFake`] runs a gRPC server on a loopback port and hands out a real [`BigQueryDb`]
//! pointed at it, so the code under test runs unchanged, through the same requests, codecs,
//! retries and errors as against BigQuery. A test scripts the fake in serde terms:
//!
//! - a query rule answers the statement it matches with rows of any `Serialize` type, DML
//!   counts, or a failure;
//! - a table holds rows, which reads serve and writes append to, and which the test reads
//!   back with [`rows`](BigQueryFake::rows);
//! - a fault makes one RPC fail with a gRPC status, a dropped connection or a call that never
//!   answers.
//!
//! There is no SQL engine: a query answers only as a rule scripts it. A call that nothing
//! answers fails with `Unimplemented`, which the client does not retry, and
//! [`verify`](BigQueryFake::verify), which runs when the fake is dropped, panics with every
//! such call and the rules that were there to answer it.
//!
//! Enable the feature in your dev-dependencies, next to an async test runtime:
//!
//! ```toml
//! [dev-dependencies]
//! bigquery = { version = "0.8", features = ["testing"] }
//! tokio = { version = "1", features = ["macros", "rt"] }
//! ```
//!
//! # Examples
//!
//! The examples share a row type and the code under test:
//!
//! ```rust
//! use bigquery::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
//! struct Order {
//!     id: i64,
//!     customer: String,
//!     total: f64,
//! }
//!
//! const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
//! const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
//! const ORDERS_OF: &str = "SELECT id, customer, total FROM shop.orders WHERE customer = @customer";
//!
//! async fn orders_of(db: &BigQueryDb, customer: &str) -> BigQueryResult<Vec<Order>> {
//!     db.fluent().query(ORDERS_OF).param("customer", customer).obj().query().await
//! }
//!
//! async fn save(db: &BigQueryDb, orders: &[Order]) -> BigQueryResult<BigQueryWriteSummary> {
//!     db.fluent().insert().into(SHOP.table(ORDERS)).objects(orders).execute().await
//! }
//! ```
//!
//! Each example below is the body of a `#[tokio::test] async fn ...() -> BigQueryResult<()>`.
//!
//! A query that returns rows:
//!
//! ```rust
//! # use bigquery::*;
//! # use serde::{Deserialize, Serialize};
//! # #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
//! # struct Order { id: i64, customer: String, total: f64 }
//! # const ORDERS_OF: &str = "SELECT id, customer, total FROM shop.orders WHERE customer = @customer";
//! # async fn orders_of(db: &BigQueryDb, customer: &str) -> BigQueryResult<Vec<Order>> {
//! #     db.fluent().query(ORDERS_OF).param("customer", customer).obj().query().await
//! # }
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> BigQueryResult<()> {
//! use bigquery::testing::BigQueryFake;
//!
//! let fake = BigQueryFake::start().await?;
//! let alice = vec![Order { id: 1, customer: "Alice".into(), total: 120.0 }];
//! let rule = fake
//!     .query(ORDERS_OF)
//!     .param("customer", "Alice")
//!     .returns_rows(|columns| columns.from_type::<Order>(), &alice)?;
//!
//! assert_eq!(orders_of(fake.db(), "Alice").await?, alice);
//! assert_eq!(rule.calls(), 1);
//! # Ok(())
//! # }
//! ```
//!
//! A writer whose rows are captured:
//!
//! ```rust
//! # use bigquery::*;
//! # use serde::{Deserialize, Serialize};
//! # #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
//! # struct Order { id: i64, customer: String, total: f64 }
//! # const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
//! # const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
//! # async fn save(db: &BigQueryDb, orders: &[Order]) -> BigQueryResult<BigQueryWriteSummary> {
//! #     db.fluent().insert().into(SHOP.table(ORDERS)).objects(orders).execute().await
//! # }
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> BigQueryResult<()> {
//! use bigquery::testing::BigQueryFake;
//!
//! let fake = BigQueryFake::start().await?;
//! fake.table(SHOP.table(ORDERS), |columns| columns.from_type::<Order>())
//!     .create()?;
//! let orders = vec![
//!     Order { id: 1, customer: "Alice".into(), total: 120.0 },
//!     Order { id: 2, customer: "Bob".into(), total: 80.5 },
//! ];
//!
//! let summary = save(fake.db(), &orders).await?;
//!
//! assert_eq!(summary.rows_written, 2);
//! let written: Vec<Order> = fake.rows(SHOP.table(ORDERS))?;
//! assert_eq!(written, orders);
//! # Ok(())
//! # }
//! ```
//!
//! A failure a retry gets past, then an outage the client gives up on. Retries against the
//! fake wait no backoff:
//!
//! ```rust
//! # use bigquery::*;
//! # use serde::{Deserialize, Serialize};
//! # #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
//! # struct Order { id: i64, customer: String, total: f64 }
//! # const ORDERS_OF: &str = "SELECT id, customer, total FROM shop.orders WHERE customer = @customer";
//! # async fn orders_of(db: &BigQueryDb, customer: &str) -> BigQueryResult<Vec<Order>> {
//! #     db.fluent().query(ORDERS_OF).param("customer", customer).obj().query().await
//! # }
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> BigQueryResult<()> {
//! use bigquery::testing::{BigQueryFake, BigQueryFakeCode, BigQueryFakeFault};
//!
//! let fake = BigQueryFake::start().await?;
//! let alice = vec![Order { id: 1, customer: "Alice".into(), total: 120.0 }];
//! let unavailable =
//!     || BigQueryFakeFault::status(BigQueryFakeCode::Unavailable, "backend went away");
//! let lost = fake.query(ORDERS_OF).times(1).fails(unavailable())?;
//! let answer = fake
//!     .query(ORDERS_OF)
//!     .param("customer", "Alice")
//!     .returns_rows(|columns| columns.from_type::<Order>(), &alice)?;
//! let outage = fake
//!     .query(ORDERS_OF)
//!     .param("customer", "Bob")
//!     .fails(unavailable())?;
//!
//! assert_eq!(orders_of(fake.db(), "Alice").await?, alice);
//! assert_eq!((lost.calls(), answer.calls()), (1, 1));
//!
//! let failed = orders_of(fake.db(), "Bob").await;
//! assert!(matches!(&failed, Err(err) if err.retry_possible()), "{failed:?}");
//! assert_eq!(outage.calls(), 4, "the first attempt and max_retries = 3 retries");
//! # Ok(())
//! # }
//! ```
//!
//! # Limits
//!
//! The fake binds its own port, so tests run in parallel. A call sees the rules registered
//! before it arrives. Tokio's paused time is not supported: a paused runtime jumps to its
//! next timer whenever it is idle, and waiting on the fake's loopback socket counts as idle.

mod admin;
mod query;
mod read;
mod rows;
mod rules;
mod schema;
mod server;
mod state;
mod write;

pub use rules::{
    BigQueryFakeCode, BigQueryFakeFault, BigQueryFakeJobFailure, BigQueryFakeRpc, BigQueryFakeRule,
};

use crate::db::fake::FakeServer;
use crate::errors::BigQueryError;
use crate::query::ParamList;
use crate::read::BigQueryBatchRows;
use crate::testing::rows::ProtoBatchBuilder;
use crate::testing::rules::{
    FaultRule, QueryAnswer, QueryReply, QueryRule, ReadRule, RejectRule, ShownParameters,
    SqlMatcher,
};
use crate::testing::server::FakeShared;
use crate::testing::state::{DatasetKey, FakeTable, TableKey};
use crate::{
    BigQueryChange, BigQueryDatasetRef, BigQueryDb, BigQueryDmlStats, BigQueryParamType,
    BigQueryResult, BigQuerySchemaColumns, BigQuerySchemaColumnsBuilder, BigQuerySql,
    BigQueryStatementType, BigQueryTableRef, BigQueryTableSchema,
};
use arrow_array::RecordBatch;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::{Debug, Formatter};
use std::num::NonZeroUsize;
use std::sync::Arc;

/// A fake BigQuery on a loopback port, and a client for it.
///
/// Start one per test with [`start`](Self::start), script it, run the code under test against
/// [`db`](Self::db), then assert on what the code returned and on what the fake recorded. The
/// fake is `Send + Sync`, so a test can share it with the tasks it spawns.
///
/// Dropping it stops the server and calls [`verify`](Self::verify).
pub struct BigQueryFake {
    db: BigQueryDb,
    shared: Arc<FakeShared>,
    /// Held for its `Drop`, which stops the server.
    _server: FakeServer,
}

// A test shares the fake with the tasks it spawns, so a field that is not `Send + Sync` must
// fail the build here rather than in a user's test.
const _: fn() = || {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<BigQueryFake>();
};

impl Debug for BigQueryFake {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BigQueryFake")
            .field("db", &self.db)
            .finish_non_exhaustive()
    }
}

impl BigQueryFake {
    /// The client for this fake, as the code under test takes it. Clones share the fake.
    pub fn db(&self) -> &BigQueryDb {
        &self.db
    }

    /// Starts a rule for the statement whose SQL equals `sql` exactly: no whitespace or case
    /// folding, so the rule matches only the text the code under test sends. A statement from
    /// [`sql_file!`](crate::sql_file!) matches by its file's text.
    pub fn query(&self, sql: impl Into<BigQuerySql>) -> BigQueryFakeQueryBuilder<'_> {
        let (sql, _) = sql.into().into_parts();
        BigQueryFakeQueryBuilder::new(self, SqlMatcher::Exact(sql))
    }

    /// Starts a rule for every statement whose SQL `sql` accepts.
    pub fn query_matching<F>(&self, sql: F) -> BigQueryFakeQueryBuilder<'_>
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        BigQueryFakeQueryBuilder::new(self, SqlMatcher::Matching(Box::new(sql)))
    }

    /// Creates the dataset, unless it exists. A dataset without a project is in the client's.
    pub fn create_dataset(&self, dataset: impl Into<BigQueryDatasetRef>) {
        let key = DatasetKey::resolve(&dataset.into(), self.shared.project());
        self.shared.state().create_dataset(key);
    }

    /// Starts a table with the columns `columns` declares, as
    /// [`BigQueryTableSchemaBuilder::columns`](crate::BigQueryTableSchemaBuilder::columns)
    /// takes them. [`create`](BigQueryFakeTableBuilder::create) creates it, and its dataset if
    /// that is missing.
    pub fn table<F, R>(
        &self,
        table: impl Into<BigQueryTableRef>,
        columns: F,
    ) -> BigQueryFakeTableBuilder<'_>
    where
        F: FnOnce(BigQuerySchemaColumnsBuilder) -> R,
        R: Into<BigQuerySchemaColumns>,
    {
        let contents = columns(BigQuerySchemaColumnsBuilder)
            .into()
            .table_schema()
            .map(|schema| FakeTableContents {
                schema,
                batches: Vec::new(),
                rows: 0,
            });
        BigQueryFakeTableBuilder {
            fake: self,
            table: table.into(),
            contents,
            read_streams: 1,
        }
    }

    /// Starts a rule for the read sessions of an existing table. A session without a row
    /// restriction is served the table's rows when no rule answers it; one with a row
    /// restriction needs a rule, since the fake does not evaluate filters.
    pub fn read(&self, table: impl Into<BigQueryTableRef>) -> BigQueryFakeReadBuilder<'_> {
        BigQueryFakeReadBuilder {
            fake: self,
            table: table.into(),
            row_restriction: None,
            times: None,
        }
    }

    /// Starts a fault for the calls of `rpc`. Faults answer before every other rule and before
    /// the tables.
    pub fn fault(&self, rpc: BigQueryFakeRpc) -> BigQueryFakeFaultBuilder<'_> {
        BigQueryFakeFaultBuilder {
            fake: self,
            rpc,
            table: None,
            times: None,
        }
    }

    /// Fails each append request to `table` that has a row `rejects` accepts, with one row
    /// error per such row, each carrying `reason`. As in BigQuery, nothing in that request is
    /// written.
    ///
    /// The rows are read as `T`, as [`rows`](Self::rows) reads them, before `rejects` sees
    /// them.
    pub fn reject_rows<T, F>(
        &self,
        table: impl Into<BigQueryTableRef>,
        reason: impl Into<String>,
        rejects: F,
    ) -> BigQueryFakeRule
    where
        T: DeserializeOwned + 'static,
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        let table = TableKey::resolve(&table.into(), self.shared.project());
        let reason = reason.into();
        let rule = BigQueryFakeRule::unlimited(format!("reject_rows on {table}: {reason:?}"));
        self.shared.rules().add_rejection(RejectRule {
            table,
            reason,
            rejects: Box::new(move |batch: &RecordBatch| {
                let mut rejected = Vec::new();
                for (index, row) in BigQueryBatchRows::<T>::new(batch).enumerate() {
                    if rejects(&row?) {
                        rejected.push(index);
                    }
                }
                Ok(rejected)
            }),
            rule: rule.clone(),
        });
        rule
    }

    /// The visible rows of `table`, in the order they were acknowledged, read as `T` the way a
    /// read session's rows are.
    ///
    /// # Errors
    /// [`BigQueryError::DataNotFoundError`] if the table does not exist, and
    /// [`BigQueryError::DeserializeError`] for a row that does not read as `T`.
    pub fn rows<T: DeserializeOwned>(
        &self,
        table: impl Into<BigQueryTableRef>,
    ) -> BigQueryResult<Vec<T>> {
        let key = TableKey::resolve(&table.into(), self.shared.project());
        let batches = self.shared.state().table(&key)?.batches.clone();
        let mut rows = Vec::new();
        let mut first_row = 0;
        for batch in &batches {
            for row in BigQueryBatchRows::<T>::numbered_from(batch, first_row) {
                rows.push(row?);
            }
            first_row += batch.num_rows() as u64;
        }
        Ok(rows)
    }

    /// The CDC changes written to `table`, in the order they were acknowledged, each row read
    /// as `T`. The fake records changes and does not apply them to the table's rows.
    ///
    /// # Errors
    /// [`BigQueryError::DataNotFoundError`] if the table does not exist, and
    /// [`BigQueryError::DeserializeError`] for a row that does not read as `T`.
    pub fn changes<T: DeserializeOwned>(
        &self,
        table: impl Into<BigQueryTableRef>,
    ) -> BigQueryResult<Vec<BigQueryChange<T>>> {
        let key = TableKey::resolve(&table.into(), self.shared.project());
        let written = self.shared.state().table(&key)?.changes.clone();
        let mut changes = Vec::new();
        let mut first_row = 0;
        for batch in &written {
            let rows = BigQueryBatchRows::<T>::numbered_from(&batch.rows, first_row);
            for (row, change) in rows.zip(&batch.changes) {
                changes.push(BigQueryChange {
                    change_type: change.change_type,
                    sequence_number: change.sequence_number.clone(),
                    row: row?,
                });
            }
            first_row += batch.rows.num_rows() as u64;
        }
        Ok(changes)
    }
}

/// A query rule being built, from [`BigQueryFake::query`] or
/// [`BigQueryFake::query_matching`]: add the parameters it expects, then pick what it answers
/// with.
///
/// The builder methods never fail. A parameter that cannot be encoded, or a
/// [`times`](Self::times) of 0, is kept, and the terminal returns the error without
/// registering the rule.
#[derive(Debug)]
pub struct BigQueryFakeQueryBuilder<'a> {
    fake: &'a BigQueryFake,
    sql: SqlMatcher,
    parameters: ParamList,
    times: Option<usize>,
    bytes_processed: Option<i64>,
}

impl<'a> BigQueryFakeQueryBuilder<'a> {
    fn new(fake: &'a BigQueryFake, sql: SqlMatcher) -> Self {
        Self {
            fake,
            sql,
            parameters: ParamList::default(),
            times: None,
            bytes_processed: None,
        }
    }
}

impl BigQueryFakeQueryBuilder<'_> {
    /// Expects the named parameter `@name`, encoded as
    /// [`BigQueryQueryBuilder::param`](crate::BigQueryQueryBuilder::param) encodes it.
    ///
    /// A rule without parameters answers a call whatever its parameters. Once it expects any,
    /// a call's named parameters must equal them as a set, and its positional ones in order.
    pub fn param<N: Into<String>, V: Serialize>(mut self, name: N, value: V) -> Self {
        self.parameters.named(&name.into(), &value);
        self
    }

    /// Expects the named parameter `@name` of type `ty`, as
    /// [`BigQueryQueryBuilder::param_as`](crate::BigQueryQueryBuilder::param_as) encodes it.
    pub fn param_as<N: Into<String>, V: Serialize>(
        mut self,
        name: N,
        ty: impl Into<BigQueryParamType>,
        value: V,
    ) -> Self {
        self.parameters.named_as(&name.into(), &ty.into(), &value);
        self
    }

    /// Expects every top-level field of a struct or string-keyed map as a named parameter, as
    /// [`BigQueryQueryBuilder::params`](crate::BigQueryQueryBuilder::params) encodes them.
    pub fn params<P: Serialize + ?Sized>(mut self, params: &P) -> Self {
        self.parameters.fields(params);
        self
    }

    /// Expects the next positional parameter `?`, as
    /// [`BigQueryQueryBuilder::positional_param`](crate::BigQueryQueryBuilder::positional_param)
    /// encodes it.
    pub fn positional_param<V: Serialize>(mut self, value: V) -> Self {
        self.parameters.positional(&value);
        self
    }

    /// Expects the next positional parameter `?` of type `ty`, as
    /// [`BigQueryQueryBuilder::positional_param_as`](crate::BigQueryQueryBuilder::positional_param_as)
    /// encodes it.
    pub fn positional_param_as<V: Serialize>(
        mut self,
        ty: impl Into<BigQueryParamType>,
        value: V,
    ) -> Self {
        self.parameters.positional_as(&ty.into(), &value);
        self
    }

    /// Answers exactly `calls` calls, retries included, and then no more, so that a later rule
    /// answers the calls after them. [`verify`](BigQueryFake::verify) fails unless the rule
    /// answered all of them. The terminal refuses 0.
    pub fn times(mut self, calls: usize) -> Self {
        self.times = Some(calls);
        self
    }

    /// The bytes the statement reports processed, in its stats and in a dry run.
    pub fn bytes_processed(mut self, bytes: i64) -> Self {
        self.bytes_processed = Some(bytes);
        self
    }

    /// Answers with `rows`, a result whose columns `columns` declares as
    /// [`BigQueryFake::table`] takes them. The same rule answers every purpose of a call: the
    /// rows for a query, the stats for `execute()`, and the schema for a dry run.
    ///
    /// # Errors
    /// The parameter or `times` error the builder kept,
    /// [`BigQueryError::SchemaInferenceError`] or
    /// [`BigQueryError::InvalidParametersError`] for columns `.plan()` would refuse, and
    /// [`BigQueryError::SerializeError`] naming the row and field of a row that does not
    /// encode as the columns say.
    pub fn returns_rows<F, R, T, I>(self, columns: F, rows: I) -> BigQueryResult<BigQueryFakeRule>
    where
        F: FnOnce(BigQuerySchemaColumnsBuilder) -> R,
        R: Into<BigQuerySchemaColumns>,
        I: IntoIterator<Item = T>,
        T: Serialize,
    {
        let schema = columns(BigQuerySchemaColumnsBuilder)
            .into()
            .table_schema()?;
        let rows = ProtoBatchBuilder::encode(&schema, rows, 0)?;
        self.register(QueryAnswer::Rows { schema, rows })
    }

    /// Answers as a DML statement of `statement_type` that changed the rows `stats` counts.
    ///
    /// # Errors
    /// The parameter or `times` error the builder kept.
    pub fn returns_dml(
        self,
        statement_type: BigQueryStatementType,
        stats: BigQueryDmlStats,
    ) -> BigQueryResult<BigQueryFakeRule> {
        self.register(QueryAnswer::Dml {
            statement_type,
            stats,
        })
    }

    /// Answers as a statement of `statement_type` with no rows and no DML counts, such as DDL.
    ///
    /// # Errors
    /// The parameter or `times` error the builder kept.
    pub fn returns_statement(
        self,
        statement_type: BigQueryStatementType,
    ) -> BigQueryResult<BigQueryFakeRule> {
        self.register(QueryAnswer::Statement(statement_type))
    }

    /// Refuses the call with `fault`.
    ///
    /// # Errors
    /// The parameter or `times` error the builder kept.
    pub fn fails(self, fault: BigQueryFakeFault) -> BigQueryResult<BigQueryFakeRule> {
        self.register(QueryAnswer::Fault(fault))
    }

    /// Runs the statement as a job that fails with `failure`: the first response says the job
    /// is still running, and the job reports the failure once it is done, which the client
    /// returns as [`BigQueryError::JobError`].
    ///
    /// # Errors
    /// The parameter or `times` error the builder kept.
    pub fn fails_job(self, failure: BigQueryFakeJobFailure) -> BigQueryResult<BigQueryFakeRule> {
        self.register(QueryAnswer::JobFailure(failure))
    }

    fn register(self, answer: QueryAnswer) -> BigQueryResult<BigQueryFakeRule> {
        let parameters = self.parameters.into_parameters()?;
        let mut description = self.sql.to_string();
        if !parameters.is_empty() {
            description.push_str(&format!(" with {}", ShownParameters(&parameters)));
        }
        let rule = BigQueryFakeRule::limited(description, self.times)?;
        self.fake.shared.rules().add_query(QueryRule {
            sql: self.sql,
            parameters: (!parameters.is_empty()).then_some(parameters),
            reply: Arc::new(QueryReply {
                answer,
                bytes_processed: self.bytes_processed,
            }),
            rule: rule.clone(),
        });
        Ok(rule)
    }
}

/// The schema of a table being built and the rows encoded for it so far.
#[derive(Debug)]
struct FakeTableContents {
    schema: BigQueryTableSchema,
    batches: Vec<RecordBatch>,
    rows: u64,
}

/// A table being built, from [`BigQueryFake::table`].
///
/// The builder methods never fail. Columns `.plan()` would refuse and rows that do not encode
/// are kept, and [`create`](Self::create) returns the first such error without creating the
/// table.
#[derive(Debug)]
pub struct BigQueryFakeTableBuilder<'a> {
    fake: &'a BigQueryFake,
    table: BigQueryTableRef,
    contents: BigQueryResult<FakeTableContents>,
    read_streams: usize,
}

impl BigQueryFakeTableBuilder<'_> {
    /// Adds `rows` after the rows added so far, encoded as a write would encode them.
    pub fn rows<T: Serialize, I: IntoIterator<Item = T>>(mut self, rows: I) -> Self {
        if let Ok(contents) = &mut self.contents {
            match ProtoBatchBuilder::encode(&contents.schema, rows, contents.rows) {
                Ok(batch) => {
                    contents.rows += batch.num_rows() as u64;
                    contents.batches.push(batch);
                }
                Err(failure) => self.contents = Err(failure),
            }
        }
        self
    }

    /// The number of streams a read session of the table has, 1 by default. The rows are
    /// spread over them. [`create`](Self::create) refuses 0.
    pub fn read_streams(mut self, streams: usize) -> Self {
        self.read_streams = streams;
        self
    }

    /// Creates the table, and its dataset if that is missing.
    ///
    /// # Errors
    /// The error the builder kept, [`BigQueryError::InvalidParametersError`] for
    /// `read_streams(0)`, and [`BigQueryError::DataConflictError`] if the table exists.
    pub fn create(self) -> BigQueryResult<()> {
        let contents = self.contents?;
        let read_streams = NonZeroUsize::new(self.read_streams).ok_or_else(|| {
            BigQueryError::invalid_parameters(
                "read_streams",
                format!("{} would have no read stream; pass 1 or more", self.table),
            )
        })?;
        let key = TableKey::resolve(&self.table, self.fake.shared.project());
        let mut state = self.fake.shared.state();
        let generation = state.next_generation();
        let table = FakeTable::new(contents.schema, contents.batches, read_streams, generation);
        state.create_table(key, table)
    }
}

/// A rule for the read sessions of one table, from [`BigQueryFake::read`].
#[derive(Debug)]
pub struct BigQueryFakeReadBuilder<'a> {
    fake: &'a BigQueryFake,
    table: BigQueryTableRef,
    row_restriction: Option<String>,
    times: Option<usize>,
}

impl BigQueryFakeReadBuilder<'_> {
    /// Matches sessions with exactly this `row_restriction`, as the code under test sends it.
    /// Unset, the rule matches sessions without one.
    pub fn row_restriction(mut self, restriction: impl Into<String>) -> Self {
        self.row_restriction = Some(restriction.into());
        self
    }

    /// Answers exactly `calls` sessions, as [`BigQueryFakeQueryBuilder::times`] does.
    pub fn times(mut self, calls: usize) -> Self {
        self.times = Some(calls);
        self
    }

    /// Answers the session with `rows` instead of the table's own, encoded as the table's
    /// columns say.
    ///
    /// # Errors
    /// [`BigQueryError::DataNotFoundError`] if the table does not exist, the `times` error the
    /// builder kept, and [`BigQueryError::SerializeError`] naming the row and field of a row
    /// that does not encode.
    pub fn returns_rows<T: Serialize, I: IntoIterator<Item = T>>(
        self,
        rows: I,
    ) -> BigQueryResult<BigQueryFakeRule> {
        let table = TableKey::resolve(&self.table, self.fake.shared.project());
        let schema = self.fake.shared.state().table(&table)?.schema.clone();
        let rows = ProtoBatchBuilder::encode(&schema, rows, 0)?;
        let description = match &self.row_restriction {
            Some(restriction) => format!("read of {table} where {restriction:?}"),
            None => format!("read of {table}"),
        };
        let rule = BigQueryFakeRule::limited(description, self.times)?;
        self.fake.shared.rules().add_read(ReadRule {
            table,
            row_restriction: self.row_restriction,
            rows,
            rule: rule.clone(),
        });
        Ok(rule)
    }
}

/// A fault being built, from [`BigQueryFake::fault`].
#[derive(Debug)]
pub struct BigQueryFakeFaultBuilder<'a> {
    fake: &'a BigQueryFake,
    rpc: BigQueryFakeRpc,
    table: Option<BigQueryTableRef>,
    times: Option<usize>,
}

impl BigQueryFakeFaultBuilder<'_> {
    /// Narrows the fault to the calls on `table`. [`respond`](Self::respond) refuses it for an
    /// RPC whose calls name no table, such as `GetJob` or the dataset RPCs.
    pub fn on_table(mut self, table: impl Into<BigQueryTableRef>) -> Self {
        self.table = Some(table.into());
        self
    }

    /// Answers exactly `calls` calls, as [`BigQueryFakeQueryBuilder::times`] does.
    pub fn times(mut self, calls: usize) -> Self {
        self.times = Some(calls);
        self
    }

    /// Answers the calls with `fault`.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for the field `on_table` when the RPC names
    /// no table, and the `times` error the builder kept.
    pub fn respond(self, fault: BigQueryFakeFault) -> BigQueryResult<BigQueryFakeRule> {
        let rpc = self.rpc;
        let table = self
            .table
            .map(|table| TableKey::resolve(&table, self.fake.shared.project()));
        if table.is_some() && !rpc.names_a_table() {
            return Err(BigQueryError::invalid_parameters(
                "on_table",
                format!("{rpc:?} calls name no table"),
            ));
        }
        let description = match &table {
            Some(table) => format!("fault on {rpc:?} of {table}"),
            None => format!("fault on {rpc:?}"),
        };
        let rule = BigQueryFakeRule::limited(description, self.times)?;
        self.fake.shared.rules().add_fault(FaultRule {
            rpc,
            table,
            fault,
            rule: rule.clone(),
        });
        Ok(rule)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Order {
        id: i64,
        customer: String,
        total: f64,
        paid: bool,
        receipt: Option<Vec<u8>>,
    }

    const ORDERS_OF: &str =
        "SELECT id, customer, total, paid, receipt FROM shop.orders WHERE customer = @customer";

    fn paid(id: i64, customer: &str, total: f64, receipt: &[u8]) -> Order {
        Order {
            id,
            customer: customer.to_string(),
            total,
            paid: true,
            receipt: Some(receipt.to_vec()),
        }
    }

    fn unpaid(id: i64, customer: &str, total: f64) -> Order {
        Order {
            id,
            customer: customer.to_string(),
            total,
            paid: false,
            receipt: None,
        }
    }

    async fn orders_of(db: &BigQueryDb, customer: &str) -> BigQueryResult<Vec<Order>> {
        db.fluent()
            .query(ORDERS_OF)
            .param("customer", customer)
            .obj()
            .query()
            .await
    }

    /// Whether dropping `fake`, which verifies it, panics.
    fn drop_panics(fake: BigQueryFake) -> bool {
        catch_unwind(AssertUnwindSafe(move || drop(fake))).is_err()
    }

    #[tokio::test]
    async fn a_rule_answers_its_query_inline() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        let orders = vec![
            paid(1, "Alice", 120.0, &[0, 159, 255]),
            unpaid(2, "Alice", -0.5),
        ];
        let rule = fake
            .query(ORDERS_OF)
            .returns_rows(|columns| columns.from_type::<Order>(), &orders)?;

        let (rows, stats) = fake
            .db()
            .fluent()
            .query(ORDERS_OF)
            .param("customer", "Alice")
            .obj::<Order>()
            .query_with_stats()
            .await?;

        assert_eq!(rows, orders);
        assert_eq!(stats.total_rows, Some(2));
        assert_eq!(stats.statement_type, Some(BigQueryStatementType::Select));
        assert_eq!(rule.calls(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn an_unmatched_query_fails_verify() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        fake.query("SELECT 1")
            .returns_rows(|columns| columns.from_type::<Order>(), Vec::<Order>::new())?;

        let unmatched = orders_of(fake.db(), "Alice").await;

        match &unmatched {
            Err(BigQueryError::DatabaseError(err)) => {
                assert_eq!(err.public.code, "Unimplemented");
                assert!(!err.retry_possible);
            }
            other => panic!("expected the unmatched call to fail, got {other:?}"),
        }
        assert!(catch_unwind(AssertUnwindSafe(|| fake.verify())).is_err());
        assert!(drop_panics(fake));
        Ok(())
    }

    #[tokio::test]
    async fn a_times_rule_not_used_up_fails_verify() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        let orders = vec![unpaid(1, "Alice", 120.0)];
        let rule = fake
            .query(ORDERS_OF)
            .times(2)
            .returns_rows(|columns| columns.from_type::<Order>(), &orders)?;

        assert_eq!(orders_of(fake.db(), "Alice").await?, orders);

        assert_eq!(rule.calls(), 1);
        assert!(drop_panics(fake));
        Ok(())
    }

    #[tokio::test]
    async fn params_must_match_once_declared() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        let alice = vec![unpaid(1, "Alice", 120.0)];
        let anyone = vec![paid(2, "Bob", 80.5, b"r-2"), unpaid(3, "Carol", 7.0)];
        let for_alice = fake
            .query(ORDERS_OF)
            .param("customer", "Alice")
            .returns_rows(|columns| columns.from_type::<Order>(), &alice)?;
        let for_anyone = fake
            .query(ORDERS_OF)
            .returns_rows(|columns| columns.from_type::<Order>(), &anyone)?;

        assert_eq!(orders_of(fake.db(), "Bob").await?, anyone);
        assert_eq!(orders_of(fake.db(), "Alice").await?, alice);

        assert_eq!((for_alice.calls(), for_anyone.calls()), (1, 1));
        Ok(())
    }
}
