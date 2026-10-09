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
            query_id: None,
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
    use crate::fluent_api::{BigQueryExprBuilder, BigQueryQueryBuilder};
    use crate::query::{infer_param, typed_param, ParamLabel};
    use crate::{
        BigQueryDatasetId, BigQueryFieldType, BigQueryReadCompression, BigQueryReadOptions,
    };
    use crate::{BigQueryLabels, BigQueryLocation, BigQueryRequestId};
    use std::time::Duration;

    #[derive(serde::Deserialize)]
    struct Row {}

    #[derive(serde::Serialize)]
    struct Filter {
        min: i64,
    }

    /// A query with every parameter form and job setting given.
    fn full_query<'a>(
        db: &'a MockDatabase,
        read_options: &BigQueryReadOptions,
    ) -> BigQueryQueryBuilder<'a, MockDatabase> {
        BigQueryExprBuilder::new(db)
            .query("SELECT @n, @min, @t")
            .param("n", 10)
            .params(&Filter { min: 3 })
            .param_as("t", BigQueryFieldType::Timestamp, None::<jiff::Timestamp>)
            .location(BigQueryLocation::from_static("EU"))
            .default_dataset(BigQueryDatasetId::from_static("shop"))
            .label("team", "data")
            .labels([("env", "test")])
            .maximum_bytes_billed(1_000_000)
            .use_query_cache(false)
            .timeout(Duration::from_secs(3))
            .job_timeout(Duration::from_secs(60))
            .request_id(BigQueryRequestId::new("req-1").expect("a request ID"))
            .inline_rows_limit(500)
            .read_options(read_options.clone())
    }

    #[tokio::test]
    async fn query_chain_passes_params_to_support() -> BigQueryResult<()> {
        let db = MockDatabase;
        let read_options =
            BigQueryReadOptions::new().with_compression(BigQueryReadCompression::Zstd);
        full_query(&db, &read_options).obj::<Row>().query().await?;
        drop(
            full_query(&db, &read_options)
                .obj::<Row>()
                .stream_query()
                .await?,
        );
        drop(
            full_query(&db, &read_options)
                .obj::<Row>()
                .stream_query_with_errors()
                .await?,
        );
        drop(full_query(&db, &read_options).record_batches().await?);
        full_query(&db, &read_options).execute().await?;
        full_query(&db, &read_options).dry_run().await?;
        BigQueryExprBuilder::new(&db)
            .query("SELECT ?")
            .positional_param("x")
            .execute()
            .await?;

        let full = BigQueryQueryParams::new("SELECT @n, @min, @t".into())
            .with_query_parameters(vec![
                infer_param(ParamLabel::Named("n"), &10)?,
                infer_param(ParamLabel::Named("min"), &3i64)?,
                typed_param(
                    ParamLabel::Named("t"),
                    &BigQueryFieldType::Timestamp.into(),
                    &None::<jiff::Timestamp>,
                )?,
            ])
            .with_location(BigQueryLocation::from_static("EU"))
            .with_default_dataset(BigQueryDatasetId::from_static("shop").into())
            .with_labels(BigQueryLabels::from([("team", "data"), ("env", "test")]))
            .with_maximum_bytes_billed(1_000_000)
            .with_use_query_cache(false)
            .with_timeout(Duration::from_secs(3))
            .with_job_timeout(Duration::from_secs(60))
            .with_request_id(BigQueryRequestId::new("req-1")?)
            .with_inline_rows_limit(500)
            .with_read_options(read_options);
        let positional = BigQueryQueryParams::new("SELECT ?".into())
            .with_query_parameters(vec![infer_param(ParamLabel::Positional(0), "x")?]);
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

    #[derive(serde::Serialize)]
    struct TopWordsFilter {
        corpus: &'static str,
        min_count: i64,
    }

    #[tokio::test]
    async fn sql_file_query_sends_the_file_text_with_its_bound_parameters() -> BigQueryResult<()> {
        let db = MockDatabase;
        BigQueryExprBuilder::new(&db)
            .query(crate::sql_file!(
                "../../query/sql/top_words.sql",
                corpus,
                min_count
            ))
            .params(&TopWordsFilter {
                corpus: "hamlet",
                min_count: 100,
            })
            .execute()
            .await?;
        let expected =
            BigQueryQueryParams::new(include_str!("../../query/sql/top_words.sql").into())
                .with_query_parameters(vec![
                    infer_param(ParamLabel::Named("corpus"), "hamlet")?,
                    infer_param(ParamLabel::Named("min_count"), &100)?,
                ]);
        assert_eq!(take_calls(), vec![("execute_query", expected)]);
        Ok(())
    }

    #[tokio::test]
    async fn sql_file_parameters_bind_whatever_the_case_of_their_names() -> BigQueryResult<()> {
        let db = MockDatabase;
        BigQueryExprBuilder::new(&db)
            .query(crate::sql_file!(
                "../../query/sql/top_words.sql",
                corpus,
                min_count
            ))
            .param("Corpus", "hamlet")
            .param("MIN_COUNT", 100)
            .execute()
            .await?;
        assert_eq!(take_calls().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn sql_file_parameter_bound_twice_in_different_case_fails_without_a_call() {
        let db = MockDatabase;
        let result = BigQueryExprBuilder::new(&db)
            .query(crate::sql_file!(
                "../../query/sql/top_words.sql",
                corpus,
                min_count
            ))
            .param("corpus", "hamlet")
            .param("min_count", 100)
            .param("Corpus", "macbeth")
            .execute()
            .await;
        match result {
            Err(BigQueryError::InvalidParametersError(err)) => {
                assert_eq!(err.public.field, "Corpus")
            }
            other => panic!("expected `Corpus` to be refused, got {other:?}"),
        }
        assert!(take_calls().is_empty());
    }

    #[tokio::test]
    async fn sql_file_parameter_left_unbound_fails_without_a_call() {
        let db = MockDatabase;
        let result = BigQueryExprBuilder::new(&db)
            .query(crate::sql_file!(
                "../../query/sql/top_words.sql",
                corpus,
                min_count
            ))
            .param("corpus", "hamlet")
            .execute()
            .await;
        match result {
            Err(BigQueryError::InvalidParametersError(err)) => {
                assert_eq!(err.public.field, "min_count")
            }
            other => panic!("expected `min_count` to be refused, got {other:?}"),
        }
        assert!(take_calls().is_empty());
    }

    #[tokio::test]
    async fn sql_file_parameter_bound_but_not_declared_fails_without_a_call() {
        let db = MockDatabase;
        let result = BigQueryExprBuilder::new(&db)
            .query(crate::sql_file!(
                "../../query/sql/top_words.sql",
                corpus,
                min_count
            ))
            .param("corpus", "hamlet")
            .param("min_count", 100)
            .param("limit", 10)
            .dry_run()
            .await;
        match result {
            Err(BigQueryError::InvalidParametersError(err)) => {
                assert_eq!(err.public.field, "limit")
            }
            other => panic!("expected `limit` to be refused, got {other:?}"),
        }
        assert!(take_calls().is_empty());
    }
}
