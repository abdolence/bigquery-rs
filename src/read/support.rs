use crate::errors::BigQueryError;
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
        self.read_table_rows(params).await?.try_collect().await
    }

    async fn stream_read_obj<'b, T>(
        &self,
        params: BigQueryReadParams,
    ) -> BigQueryResult<BoxStream<'b, T>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let rows = self.read_table_rows(params).await?;
        Ok(skip_failed_rows(rows))
    }

    async fn stream_read_obj_with_errors<'b, T>(
        &self,
        params: BigQueryReadParams,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<T>>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        self.read_table_rows(params).await
    }

    async fn stream_read_record_batches<'b>(
        &self,
        params: BigQueryReadParams,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<RecordBatch>>> {
        self.read_table_batches(params).await
    }
}

/// `rows` without the rows that could not be read, each logged as it is skipped.
///
/// A decode failure is logged by its kind, row and field path only: its message can hold the
/// cell's text, which may be anything the table stores. The full error stays on the `Err` items
/// of the `_with_errors` streams.
pub(crate) fn skip_failed_rows<'b, T: Send + 'b>(
    rows: BoxStream<'b, BigQueryResult<T>>,
) -> BoxStream<'b, T> {
    rows.filter_map(|row| futures::future::ready(skip_failed_row(row)))
        .boxed()
}

fn skip_failed_row<T>(row: BigQueryResult<T>) -> Option<T> {
    match row {
        Ok(row) => Some(row),
        Err(BigQueryError::DeserializeError(err)) => {
            error!(
                kind = err.kind.code(),
                row = err.row,
                path = %err.path,
                "Failed to decode a row. It is skipped."
            );
            None
        }
        Err(err) => {
            error!(%err, "Failed to read a row. It is skipped.");
            None
        }
    }
}
