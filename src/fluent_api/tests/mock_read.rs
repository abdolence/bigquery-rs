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
    use crate::fluent_api::{BigQueryExprBuilder, BigQuerySelectBuilder};
    use crate::paths;
    use crate::{
        BigQueryDatasetId, BigQueryDatasetRef, BigQueryReadCompression, BigQueryReadOptions,
        BigQueryTableId,
    };

    const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
    const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");

    #[derive(serde::Deserialize)]
    struct Row {
        name: String,
        n: i64,
    }

    /// A select of `Row` from `acme-prod.shop.orders` with every read setting given.
    fn full_select<'a>(
        db: &'a MockDatabase,
        at: jiff::Timestamp,
        options: &BigQueryReadOptions,
    ) -> BigQuerySelectBuilder<'a, MockDatabase> {
        BigQueryExprBuilder::new(db)
            .select()
            .fields(paths!(Row::{name, n}))
            .from(
                BigQueryDatasetRef::new("acme-prod", SHOP)
                    .expect("valid test input")
                    .table(ORDERS),
            )
            .filter_sql("n > 10")
            .snapshot_time(at)
            .sample_percentage(50.0)
            .options(options.clone())
    }

    /// A select whose filter names a field path the crate refuses.
    fn refused_filter_select(db: &MockDatabase) -> BigQuerySelectBuilder<'_, MockDatabase> {
        BigQueryExprBuilder::new(db)
            .select()
            .from(SHOP.table(ORDERS))
            .filter(|f| f.field("n\0").eq(1))
    }

    fn assert_invalid_parameters(result: BigQueryResult<()>) {
        assert!(
            matches!(
                result,
                Err(crate::errors::BigQueryError::InvalidParametersError(_))
            ),
            "{result:?}"
        )
    }

    #[tokio::test]
    async fn select_chain_passes_params_to_support() -> BigQueryResult<()> {
        let db = MockDatabase;
        let at: jiff::Timestamp = "2026-10-04T12:00:00Z".parse().expect("valid");
        let options = BigQueryReadOptions::new()
            .with_max_stream_count(4)
            .with_compression(BigQueryReadCompression::Zstd);
        full_select(&db, at, &options).obj::<Row>().query().await?;
        drop(
            full_select(&db, at, &options)
                .obj::<Row>()
                .stream_query()
                .await?,
        );
        drop(
            full_select(&db, at, &options)
                .obj::<Row>()
                .stream_query_with_errors()
                .await?,
        );
        drop(full_select(&db, at, &options).record_batches().await?);
        drop(
            BigQueryExprBuilder::new(&db)
                .select()
                .from(SHOP.table(ORDERS))
                .record_batches()
                .await?,
        );

        let full = BigQueryReadParams::new(
            BigQueryDatasetRef::new("acme-prod", SHOP)
                .expect("valid test input")
                .table(ORDERS),
        )
        .with_selected_fields(vec!["name".into(), "n".into()])
        .with_row_restriction("n > 10".into())
        .with_snapshot_time(at)
        .with_sample_percentage(50.0)
        .with_options(options);
        let bare = BigQueryReadParams::new(SHOP.table(ORDERS));
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

    #[tokio::test]
    async fn filter_sends_the_rendered_restriction() -> BigQueryResult<()> {
        let db = MockDatabase;
        BigQueryExprBuilder::new(&db)
            .select()
            .from(SHOP.table(ORDERS))
            .filter(|f| {
                f.for_all([
                    f.field(crate::path!(Row::name)).eq("x' OR TRUE --"),
                    f.field(crate::path!(Row::n)).gt(10),
                ])
            })
            .obj::<Row>()
            .query()
            .await?;
        BigQueryExprBuilder::new(&db)
            .select()
            .from(SHOP.table(ORDERS))
            .filter_sql("n > 10")
            .filter(|f| f.for_all([None::<crate::BigQueryFilter>]))
            .obj::<Row>()
            .query()
            .await?;
        let restrictions: Vec<Option<String>> = take_calls()
            .into_iter()
            .map(|(_, params)| params.row_restriction)
            .collect();
        assert_eq!(
            restrictions,
            [
                Some("`name` = 'x\\' OR TRUE --' AND `n` > 10".to_string()),
                None
            ],
            "a filter that builds to None clears the earlier one"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_refused_filter_fails_every_terminal_before_any_call() {
        let db = MockDatabase;
        assert_invalid_parameters(
            refused_filter_select(&db)
                .obj::<Row>()
                .query()
                .await
                .map(drop),
        );
        assert_invalid_parameters(
            refused_filter_select(&db)
                .obj::<Row>()
                .stream_query()
                .await
                .map(drop),
        );
        assert_invalid_parameters(
            refused_filter_select(&db)
                .obj::<Row>()
                .stream_query_with_errors()
                .await
                .map(drop),
        );
        assert_invalid_parameters(refused_filter_select(&db).record_batches().await.map(drop));
        assert!(take_calls().is_empty(), "nothing is sent");

        let replaced = refused_filter_select(&db)
            .filter_sql("n > 1")
            .record_batches()
            .await;
        assert!(replaced.is_ok(), "the last filter call wins");
    }
}
