use crate::read::{read_table_batches, read_table_rows};
use crate::{BigQueryDb, BigQueryReadParams, BigQueryReadSupport, BigQueryResult};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use serde::de::DeserializeOwned;
use tracing::error;

#[async_trait]
impl BigQueryReadSupport for BigQueryDb {
    async fn read_obj<T>(&self, params: BigQueryReadParams) -> BigQueryResult<Vec<T>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        read_table_rows(self, params).await?.try_collect().await
    }

    async fn stream_read_obj<'b, T>(
        &self,
        params: BigQueryReadParams,
    ) -> BigQueryResult<BoxStream<'b, T>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let rows = read_table_rows(self, params).await?;
        Ok(rows
            .filter_map(|row| {
                futures::future::ready(match row {
                    Ok(row) => Some(row),
                    Err(err) => {
                        error!(%err, "Failed to read a row. It is skipped.");
                        None
                    }
                })
            })
            .boxed())
    }

    async fn stream_read_obj_with_errors<'b, T>(
        &self,
        params: BigQueryReadParams,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<T>>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        read_table_rows(self, params).await
    }

    async fn stream_read_record_batches<'b>(
        &self,
        params: BigQueryReadParams,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<RecordBatch>>> {
        read_table_batches(self, params).await
    }
}
