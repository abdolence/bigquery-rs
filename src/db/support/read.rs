use crate::{BigQueryReadParams, BigQueryResult};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::de::DeserializeOwned;

/// Table reads through Storage Read.
#[async_trait]
pub trait BigQueryReadSupport {
    /// Reads every row into a `Vec`, failing on the first error.
    async fn read_obj<T>(&self, params: BigQueryReadParams) -> BigQueryResult<Vec<T>>
    where
        T: DeserializeOwned + Send + 'static;

    /// Streams the rows, skipping and logging every failed one.
    async fn stream_read_obj<'b, T>(
        &self,
        params: BigQueryReadParams,
    ) -> BigQueryResult<BoxStream<'b, T>>
    where
        T: DeserializeOwned + Send + 'static;

    /// Streams the rows, yielding every failure as an `Err` item.
    async fn stream_read_obj_with_errors<'b, T>(
        &self,
        params: BigQueryReadParams,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<T>>>
    where
        T: DeserializeOwned + Send + 'static;

    /// Streams the record batches as BigQuery sent them.
    async fn stream_read_record_batches<'b>(
        &self,
        params: BigQueryReadParams,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<RecordBatch>>>;
}
