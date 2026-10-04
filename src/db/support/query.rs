use crate::{BigQueryDryRunResult, BigQueryQueryOutcome, BigQueryQueryParams, BigQueryResult};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::de::DeserializeOwned;

/// Queries through `JobService.Query`, with large results read through Storage Read.
#[async_trait]
pub trait BigQueryQuerySupport {
    /// Collects the result rows into a `Vec`, failing on the first error.
    async fn query_obj<T>(&self, params: BigQueryQueryParams) -> BigQueryResult<Vec<T>>
    where
        T: DeserializeOwned + Send + 'static;

    /// Streams the result rows, skipping and logging every failed one.
    async fn stream_query_obj<'b, T>(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<BoxStream<'b, T>>
    where
        T: DeserializeOwned + Send + 'static;

    /// Streams the result rows, yielding every failure as an `Err` item.
    async fn stream_query_obj_with_errors<'b, T>(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<T>>>
    where
        T: DeserializeOwned + Send + 'static;

    /// Streams the result as record batches.
    async fn query_record_batches<'b>(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<RecordBatch>>>;

    /// Runs the statement and reports what it did, reading no rows.
    async fn execute_query(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<BigQueryQueryOutcome>;

    /// Validates the statement and reports its cost without running it.
    async fn dry_run_query(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<BigQueryDryRunResult>;
}
