use crate::{BigQueryResult, BigQueryTableRef};
use async_trait::async_trait;

/// Reads table metadata through the v2 `TableService`.
#[async_trait]
pub trait BigQueryTableSupport {
    /// The columns of `table`'s primary key, in key order; empty for a table without one.
    async fn primary_key_columns(&self, table: &BigQueryTableRef) -> BigQueryResult<Vec<String>>;
}
