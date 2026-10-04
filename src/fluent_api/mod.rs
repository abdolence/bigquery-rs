//! A fluent, chainable API for building and running BigQuery operations.
//!
//! Start from [`BigQueryDb::fluent()`](crate::BigQueryDb::fluent).

use crate::{BigQueryDb, BigQueryQueryParams};
use crate::{BigQueryQuerySupport, BigQueryReadSupport, BigQueryWriteSupport};

mod insert_builder;
mod query_builder;
mod select_builder;

pub use insert_builder::*;
pub use query_builder::*;
pub use select_builder::*;

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
