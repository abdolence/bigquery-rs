//! `BigQueryReadSupport` for `MockDatabase`: every call records the params it was given on the
//! calling thread and answers with no rows.

use super::mockdb::MockDatabase;
use crate::{BigQueryReadParams, BigQueryReadSupport, BigQueryResult};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use serde::de::DeserializeOwned;
use std::cell::RefCell;

thread_local! {
    static CALLS: RefCell<Vec<(&'static str, BigQueryReadParams)>> = const { RefCell::new(Vec::new()) };
}

fn record(call: &'static str, params: BigQueryReadParams) {
    CALLS.with(|calls| calls.borrow_mut().push((call, params)));
}

/// The calls recorded on this thread so far, oldest first, and forgets them.
fn take_calls() -> Vec<(&'static str, BigQueryReadParams)> {
    CALLS.with(|calls| calls.take())
}

#[async_trait]
impl BigQueryReadSupport for MockDatabase {
    async fn read_obj<T>(&self, params: BigQueryReadParams) -> BigQueryResult<Vec<T>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        record("read_obj", params);
        Ok(Vec::new())
    }

    async fn stream_read_obj<'b, T>(
        &self,
        params: BigQueryReadParams,
    ) -> BigQueryResult<BoxStream<'b, T>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        record("stream_read_obj", params);
        Ok(futures::stream::empty().boxed())
    }

    async fn stream_read_obj_with_errors<'b, T>(
        &self,
        params: BigQueryReadParams,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<T>>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        record("stream_read_obj_with_errors", params);
        Ok(futures::stream::empty().boxed())
    }

    async fn stream_read_record_batches<'b>(
        &self,
        params: BigQueryReadParams,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<RecordBatch>>> {
        record("stream_read_record_batches", params);
        Ok(futures::stream::empty().boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fluent_api::BigQueryExprBuilder;
    use crate::paths;
    use crate::{BigQueryReadCompression, BigQueryReadOptions, BigQueryTableRef};

    #[derive(serde::Deserialize)]
    struct Row {
        name: String,
        n: i64,
    }

    #[tokio::test]
    async fn select_chain_passes_params_to_support() -> BigQueryResult<()> {
        let db = MockDatabase;
        let at: jiff::Timestamp = "2026-10-04T12:00:00Z".parse().expect("valid");
        let options = BigQueryReadOptions::new()
            .with_max_stream_count(4)
            .with_compression(BigQueryReadCompression::Zstd);
        let select = || {
            BigQueryExprBuilder::new(&db)
                .select()
                .fields(paths!(Row::{name, n}))
                .from(("p", "ds", "t"))
                .filter("n > 10")
                .snapshot_time(at)
                .sample_percentage(50.0)
                .options(options.clone())
        };
        select().obj::<Row>().query().await?;
        drop(select().obj::<Row>().stream_query().await?);
        drop(select().obj::<Row>().stream_query_with_errors().await?);
        drop(select().record_batches().await?);
        drop(
            BigQueryExprBuilder::new(&db)
                .select()
                .from(("ds", "t"))
                .record_batches()
                .await?,
        );

        let full = BigQueryReadParams::new(BigQueryTableRef::from(("p", "ds", "t")))
            .with_selected_fields(vec!["name".into(), "n".into()])
            .with_row_restriction("n > 10".into())
            .with_snapshot_time(at)
            .with_sample_percentage(50.0)
            .with_options(options);
        let bare = BigQueryReadParams::new(BigQueryTableRef::from(("ds", "t")));
        assert_eq!(
            take_calls(),
            vec![
                ("read_obj", full.clone()),
                ("stream_read_obj", full.clone()),
                ("stream_read_obj_with_errors", full.clone()),
                ("stream_read_record_batches", full),
                ("stream_read_record_batches", bare),
            ]
        );
        Ok(())
    }
}
