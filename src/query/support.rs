use crate::query::routing::Rows;
use crate::read::{decode_rows, skip_failed_rows};
use crate::{
    BigQueryDb, BigQueryDryRunResult, BigQueryJobStats, BigQueryQueryOutcome, BigQueryQueryParams,
    BigQueryQuerySupport, BigQueryReadParams, BigQueryResult,
};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use serde::de::DeserializeOwned;
use tracing::Instrument;

#[async_trait]
impl BigQueryQuerySupport for BigQueryDb {
    async fn query_obj<T>(&self, params: BigQueryQueryParams) -> BigQueryResult<Vec<T>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        Ok(self.query_obj_with_stats(params).await?.0)
    }

    async fn query_obj_with_stats<T>(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<(Vec<T>, BigQueryJobStats)>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let (rows, stats) = self.stream_query_obj_with_stats(params).await?;
        Ok((rows.try_collect().await?, stats))
    }

    async fn stream_query_obj<'b, T>(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<BoxStream<'b, T>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let rows = self.stream_query_obj_with_errors(params).await?;
        Ok(skip_failed_rows(rows))
    }

    async fn stream_query_obj_with_errors<'b, T>(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<T>>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        Ok(self.stream_query_obj_with_stats(params).await?.0)
    }

    async fn stream_query_obj_with_stats<'b, T>(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<(BoxStream<'b, BigQueryResult<T>>, BigQueryJobStats)>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let span = params.span();
        let (rows, stats) = self
            .query_rows(&params, &span)
            .instrument(span.clone())
            .await?;
        let rows = match rows {
            Rows::Inline(batch) => futures::stream::iter(decode_rows::<T>(&batch, 0)).boxed(),
            Rows::None => futures::stream::empty().boxed(),
            Rows::Table(table) => {
                let read = BigQueryReadParams::new(table).with_options(params.read_options);
                self.read_table_rows(read).await?
            }
        };
        Ok((rows, stats))
    }

    async fn query_record_batches<'b>(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<RecordBatch>>> {
        let span = params.span();
        match self
            .query_rows(&params, &span)
            .instrument(span.clone())
            .await?
            .0
        {
            Rows::Inline(batch) => Ok(futures::stream::once(async { Ok(batch) }).boxed()),
            Rows::None => Ok(futures::stream::empty().boxed()),
            Rows::Table(table) => {
                let read = BigQueryReadParams::new(table).with_options(params.read_options);
                self.read_table_batches(read).await
            }
        }
    }

    async fn execute_query(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<BigQueryQueryOutcome> {
        let span = params.span();
        self.execute_statement(&params, &span)
            .instrument(span.clone())
            .await
    }

    async fn dry_run_query(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<BigQueryDryRunResult> {
        let span = params.span();
        self.dry_run_statement(&params, &span)
            .instrument(span.clone())
            .await
    }
}
