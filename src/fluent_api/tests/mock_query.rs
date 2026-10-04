//! `BigQueryQuerySupport` for `MockDatabase`: every call records the params it was given on the
//! calling thread and answers with no rows and an empty outcome.

use super::mockdb::MockDatabase;
use crate::{
    BigQueryDryRunResult, BigQueryJobStats, BigQueryQueryOutcome, BigQueryQueryParams,
    BigQueryQuerySupport, BigQueryResult,
};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use serde::de::DeserializeOwned;
use std::cell::RefCell;

thread_local! {
    static CALLS: RefCell<Vec<(&'static str, BigQueryQueryParams)>> = const { RefCell::new(Vec::new()) };
}

fn record(call: &'static str, params: BigQueryQueryParams) {
    CALLS.with(|calls| calls.borrow_mut().push((call, params)));
}

/// The calls recorded on this thread so far, oldest first, and forgets them.
fn take_calls() -> Vec<(&'static str, BigQueryQueryParams)> {
    CALLS.with(|calls| calls.take())
}

#[async_trait]
impl BigQueryQuerySupport for MockDatabase {
    async fn query_obj<T>(&self, params: BigQueryQueryParams) -> BigQueryResult<Vec<T>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        record("query_obj", params);
        Ok(Vec::new())
    }

    async fn stream_query_obj<'b, T>(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<BoxStream<'b, T>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        record("stream_query_obj", params);
        Ok(futures::stream::empty().boxed())
    }

    async fn stream_query_obj_with_errors<'b, T>(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<T>>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        record("stream_query_obj_with_errors", params);
        Ok(futures::stream::empty().boxed())
    }

    async fn query_obj_with_stats<T>(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<(Vec<T>, BigQueryJobStats)>
    where
        T: DeserializeOwned + Send + 'static,
    {
        record("query_obj_with_stats", params);
        Ok((Vec::new(), BigQueryJobStats::default()))
    }

    async fn stream_query_obj_with_stats<'b, T>(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<(BoxStream<'b, BigQueryResult<T>>, BigQueryJobStats)>
    where
        T: DeserializeOwned + Send + 'static,
    {
        record("stream_query_obj_with_stats", params);
        Ok((
            futures::stream::empty().boxed(),
            BigQueryJobStats::default(),
        ))
    }

    async fn query_record_batches<'b>(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<RecordBatch>>> {
        record("query_record_batches", params);
        Ok(futures::stream::empty().boxed())
    }

    async fn execute_query(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<BigQueryQueryOutcome> {
        record("execute_query", params);
        Ok(BigQueryQueryOutcome {
            job: None,
            statement_type: None,
            num_dml_affected_rows: None,
            dml_stats: None,
            total_rows: None,
            total_bytes_processed: None,
            total_bytes_billed: None,
            total_slot_ms: None,
            cache_hit: None,
        })
    }

    async fn dry_run_query(
        &self,
        params: BigQueryQueryParams,
    ) -> BigQueryResult<BigQueryDryRunResult> {
        record("dry_run_query", params);
        Ok(BigQueryDryRunResult {
            total_bytes_processed: None,
            schema: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::BigQueryError;
    use crate::fluent_api::BigQueryExprBuilder;
    use crate::query::{infer_param, typed_param, ParamLabel};
    use crate::{
        BigQueryDatasetId, BigQueryFieldType, BigQueryReadCompression, BigQueryReadOptions,
    };
    use std::collections::BTreeMap;
    use std::time::Duration;

    #[derive(serde::Deserialize)]
    struct Row {}

    #[derive(serde::Serialize)]
    struct Filter {
        min: i64,
    }

    #[tokio::test]
    async fn query_chain_passes_params_to_support() -> BigQueryResult<()> {
        let db = MockDatabase;
        let read_options =
            BigQueryReadOptions::new().with_compression(BigQueryReadCompression::Zstd);
        let query = || {
            BigQueryExprBuilder::new(&db)
                .query("SELECT @n, @min, @t")
                .param("n", 10)
                .params(&Filter { min: 3 })
                .param_as("t", BigQueryFieldType::Timestamp, None::<jiff::Timestamp>)
                .location("EU")
                .default_dataset(BigQueryDatasetId::from_static("ds"))
                .label("team", "data")
                .labels([("env", "test")])
                .maximum_bytes_billed(1_000_000)
                .use_query_cache(false)
                .timeout(Duration::from_secs(3))
                .job_timeout(Duration::from_secs(60))
                .request_id("req-1")
                .inline_rows_limit(500)
                .read_options(read_options.clone())
        };
        query().obj::<Row>().query().await?;
        drop(query().obj::<Row>().stream_query().await?);
        drop(query().obj::<Row>().stream_query_with_errors().await?);
        drop(query().record_batches().await?);
        query().execute().await?;
        query().dry_run().await?;
        BigQueryExprBuilder::new(&db)
            .query("SELECT ?")
            .positional_param("x")
            .execute()
            .await?;

        let full = BigQueryQueryParams::new("SELECT @n, @min, @t".into())
            .with_query_parameters(vec![
                infer_param(ParamLabel::Named("n"), &10).map_err(BigQueryError::from)?,
                infer_param(ParamLabel::Named("min"), &3i64).map_err(BigQueryError::from)?,
                typed_param(
                    ParamLabel::Named("t"),
                    &BigQueryFieldType::Timestamp.into(),
                    &None::<jiff::Timestamp>,
                )
                .map_err(BigQueryError::from)?,
            ])
            .with_location("EU".into())
            .with_default_dataset(BigQueryDatasetId::from_static("ds").into())
            .with_labels(BTreeMap::from([
                ("team".to_string(), "data".to_string()),
                ("env".to_string(), "test".to_string()),
            ]))
            .with_maximum_bytes_billed(1_000_000)
            .with_use_query_cache(false)
            .with_timeout(Duration::from_secs(3))
            .with_job_timeout(Duration::from_secs(60))
            .with_request_id("req-1".into())
            .with_inline_rows_limit(500)
            .with_read_options(read_options);
        let positional =
            BigQueryQueryParams::new("SELECT ?".into()).with_query_parameters(vec![infer_param(
                ParamLabel::Positional(0),
                "x",
            )
            .map_err(BigQueryError::from)?]);
        assert_eq!(
            take_calls(),
            vec![
                ("query_obj", full.clone()),
                ("stream_query_obj", full.clone()),
                ("stream_query_obj_with_errors", full.clone()),
                ("query_record_batches", full.clone()),
                ("execute_query", full.clone()),
                ("dry_run_query", full),
                ("execute_query", positional),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn parameter_failure_is_returned_by_the_terminal_without_a_call() {
        let db = MockDatabase;
        let result = BigQueryExprBuilder::new(&db)
            .query("SELECT @a, @b")
            .param("a", None::<i64>)
            .param("b", 1)
            .execute()
            .await;
        match result {
            Err(BigQueryError::InvalidParametersError(err)) => assert_eq!(err.public.field, "a"),
            other => panic!("expected the failure of `a`, got {other:?}"),
        }
        assert!(take_calls().is_empty());
    }
}
