use crate::{BigQueryResult, BigQueryTableDeclaration, BigQueryTablePlan, BigQueryTableSyncReport};
use async_trait::async_trait;

/// Declarative table schemas, planned and synced through the v2 `TableService` and DDL.
#[async_trait]
pub trait BigQuerySchemaSupport {
    /// Reads the table and reports what [`sync_table_schema`](Self::sync_table_schema) would
    /// do, writing nothing.
    async fn plan_table_schema(
        &self,
        declaration: BigQueryTableDeclaration,
    ) -> BigQueryResult<BigQueryTablePlan>;

    /// Reads the table and applies the plan.
    async fn sync_table_schema(
        &self,
        declaration: BigQueryTableDeclaration,
    ) -> BigQueryResult<BigQueryTableSyncReport>;
}
