//! A fluent, chainable API for building and running BigQuery operations.
//!
//! Start from [`BigQueryDb::fluent()`](crate::BigQueryDb::fluent).

use crate::{BigQueryDb, BigQueryQueryParams};
use crate::{
    BigQueryQuerySupport, BigQueryReadSupport, BigQuerySchemaSupport, BigQueryWriteSupport,
};

mod insert_builder;
mod query_builder;
mod schema_builder;
mod select_builder;
mod select_filter_builder;

pub use insert_builder::*;
pub use query_builder::*;
pub use schema_builder::*;
pub use select_builder::*;
pub use select_filter_builder::*;

/// The entry point for fluent BigQuery operations, obtained from
/// [`BigQueryDb::fluent()`](crate::BigQueryDb::fluent).
///
/// The type parameter `D` is an internal implementation detail; outside this crate's tests it is
/// always [`BigQueryDb`](crate::BigQueryDb).
#[derive(Clone, Debug)]
pub struct BigQueryExprBuilder<'a, D> {
    db: &'a D,
}

impl<'a, D> BigQueryExprBuilder<'a, D> {
    pub(crate) fn new(db: &'a D) -> Self {
        Self { db }
    }
}

// Each operation sits in its own impl block, bounded only by its own support trait, so that no
// operation widens the bounds another one pays for.
impl<'a, D> BigQueryExprBuilder<'a, D>
where
    D: BigQueryReadSupport + Clone + Send + Sync + 'static,
{
    /// Starts a table read through the Storage Read API. Continue with `.fields()` to pick the
    /// columns, or `.from()` to name the table.
    #[inline]
    pub fn select(self) -> BigQuerySelectInitialBuilder<'a, D> {
        BigQuerySelectInitialBuilder::new(self.db)
    }
}

impl<'a, D> BigQueryExprBuilder<'a, D>
where
    D: BigQueryQuerySupport + Clone + Send + Sync + 'static,
{
    /// Starts a GoogleSQL query. Continue with its parameters and settings, then a terminal.
    #[inline]
    pub fn query(self, sql: impl Into<String>) -> BigQueryQueryBuilder<'a, D> {
        BigQueryQueryBuilder::new(self.db, BigQueryQueryParams::new(sql.into()))
    }
}

impl<'a, D> BigQueryExprBuilder<'a, D>
where
    D: BigQueryWriteSupport + Clone + Send + Sync + 'static,
{
    /// Starts an insert through the Storage Write API. Continue with `.into()` to name the
    /// table.
    #[inline]
    pub fn insert(self) -> BigQueryInsertInitialBuilder<'a, D> {
        BigQueryInsertInitialBuilder::new(self.db)
    }
}

impl<'a, D> BigQueryExprBuilder<'a, D>
where
    D: BigQuerySchemaSupport + Clone + Send + Sync + 'static,
{
    /// Starts a schema declaration. Continue with `.table()` to declare one table's columns
    /// and settings, then `.plan()` or `.sync()`.
    ///
    /// ```rust,no_run
    /// # use bigquery::*;
    /// # async fn example(db: BigQueryDb) -> BigQueryResult<()> {
    /// const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
    /// const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
    ///
    /// struct Order {
    ///     id: i64,
    ///     customer: String,
    ///     placed_at: jiff::Timestamp,
    /// }
    ///
    /// let report = db
    ///     .fluent()
    ///     .schema()
    ///     .table(SHOP.table(ORDERS))
    ///     .columns(|c| {
    ///         c.fields([
    ///             c.field(path!(Order::id)).int64().required(),
    ///             c.field(path!(Order::customer)).string(),
    ///             c.field(path!(Order::placed_at)).timestamp(),
    ///         ])
    ///     })
    ///     .primary_key([path!(Order::id)])
    ///     .partition_by_day(path!(Order::placed_at))
    ///     .cluster_by([path!(Order::customer)])
    ///     .sync()
    ///     .await?;
    /// println!("{report}");
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn schema(self) -> BigQuerySchemaBuilder<'a, D> {
        BigQuerySchemaBuilder::new(self.db)
    }
}

impl BigQueryDb {
    /// Starts a fluent BigQuery operation.
    #[inline]
    pub fn fluent(&self) -> BigQueryExprBuilder<'_, BigQueryDb> {
        BigQueryExprBuilder::new(self)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    pub mod mockdb;

    mod mock_query;
    mod mock_read;
    mod mock_write;
}
