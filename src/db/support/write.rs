use crate::{BigQueryChange, BigQueryInsertParams, BigQueryResult, BigQueryWriteSummary};
use async_trait::async_trait;
use serde::Serialize;

/// Inserts through Storage Write.
#[async_trait]
pub trait BigQueryWriteSupport {
    /// Writes every row through one writer and finishes it.
    async fn insert_objects<T, I>(
        &self,
        params: BigQueryInsertParams,
        rows: I,
    ) -> BigQueryResult<BigQueryWriteSummary>
    where
        T: Serialize + Send + Sync,
        I: IntoIterator<Item = T> + Send,
        I::IntoIter: Send;

    /// Writes every CDC change through one CDC writer and finishes it.
    async fn insert_changes<T, I>(
        &self,
        params: BigQueryInsertParams,
        changes: I,
    ) -> BigQueryResult<BigQueryWriteSummary>
    where
        T: Serialize + Send + Sync,
        I: IntoIterator<Item = BigQueryChange<T>> + Send,
        I::IntoIter: Send;
}
