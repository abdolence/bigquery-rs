use crate::query::routing::{dry_run, execute, query_rows, query_span, Rows};
use crate::read::{decode_rows, read_table_batches, read_table_rows, skip_failed_row};
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
        Ok(rows
            .filter_map(|row| futures::future::ready(skip_failed_row(row)))
            .boxed())
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
        let span = query_span(&params);
        let (rows, stats) = query_rows(self, &params, &span)
            .instrument(span.clone())
            .await?;
        let rows = match rows {
            Rows::Inline(Some(batch)) => futures::stream::iter(decode_rows::<T>(&batch, 0)).boxed(),
            Rows::Inline(None) | Rows::None => futures::stream::empty().boxed(),
            Rows::Table(table) => {
                let read = BigQueryReadParams::new(table).with_options(params.read_options);
                read_table_rows(self, read).await?
            }
        };
        Ok((rows, stats))
    }

    async fn query_record_batches<'b>(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<RecordBatch>>> {
        let span = query_span(&params);
        match query_rows(self, &params, &span)
            .instrument(span.clone())
            .await?
            .0
        {
            Rows::Inline(Some(batch)) => Ok(futures::stream::once(async { Ok(batch) }).boxed()),
            Rows::Inline(None) | Rows::None => Ok(futures::stream::empty().boxed()),
            Rows::Table(table) => {
                let read = BigQueryReadParams::new(table).with_options(params.read_options);
                read_table_batches(self, read).await
            }
        }
    }

    async fn execute_query(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<BigQueryQueryOutcome> {
        let span = query_span(&params);
        execute(self, &params, &span).instrument(span.clone()).await
    }

    async fn dry_run_query(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<BigQueryDryRunResult> {
        let span = query_span(&params);
        dry_run(self, &params, &span).instrument(span.clone()).await
    }
}
