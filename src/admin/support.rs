//! [`BigQueryTableSupport`] for [`BigQueryDb`]: one `GetTable` per lookup.

use crate::{BigQueryDb, BigQueryResult, BigQueryTableRef, BigQueryTableSupport};
use async_trait::async_trait;

#[async_trait]
impl BigQueryTableSupport for BigQueryDb {
    async fn primary_key_columns(&self, table: &BigQueryTableRef) -> BigQueryResult<Vec<String>> {
        Ok(self
            .get_table_body(table)
            .await?
            .table_constraints
            .and_then(|constraints| constraints.primary_key)
            .map(|key| key.columns)
            .unwrap_or_default())
    }
}
