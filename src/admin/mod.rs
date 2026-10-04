//! Plain admin calls over the v2 API: datasets, tables and jobs, returned as typed values.
//!
//! Datasets and tables are reached through
//! [`BigQueryExprBuilder::schema`](crate::BigQueryExprBuilder::schema), next to the
//! declarative table schemas; jobs through methods on [`BigQueryDb`](crate::BigQueryDb).

mod dataset;
pub use dataset::*;

mod table;
pub use table::*;

mod job;
pub use job::*;

use crate::BigQueryResult;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use std::future::Future;
use tracing::error;

/// Streams every item of a paged listing, fetching each page as the stream reaches it.
///
/// `fetch` gets the page token, empty for the first page, and returns the page's items and the
/// next token, empty after the last page. A failed page is yielded as the stream's last item,
/// since the listing cannot go on without the token that page would have returned.
pub(crate) fn paged<'b, T, F, Fut>(fetch: F) -> BoxStream<'b, BigQueryResult<T>>
where
    T: Send + 'b,
    F: FnMut(String) -> Fut + Send + 'b,
    Fut: Future<Output = BigQueryResult<(Vec<T>, String)>> + Send + 'b,
{
    futures::stream::try_unfold(
        (fetch, Some(String::new())),
        |(mut fetch, token)| async move {
            let Some(token) = token else {
                return BigQueryResult::Ok(None);
            };
            let (items, next) = fetch(token).await?;
            let next = (!next.is_empty()).then_some(next);
            Ok(Some((items, (fetch, next))))
        },
    )
    .map_ok(|items| futures::stream::iter(items.into_iter().map(Ok)))
    .try_flatten()
    .boxed()
}

/// `listing` without its error: the error is logged, and it ends the stream.
pub(crate) fn logging_errors<'b, T>(
    listing: BoxStream<'b, BigQueryResult<T>>,
    what: &'static str,
) -> BoxStream<'b, T>
where
    T: Send + 'b,
{
    listing
        .filter_map(move |item| async move {
            match item {
                Ok(item) => Some(item),
                Err(err) => {
                    error!(%err, "Failed to list {what}; the listing ends here.");
                    None
                }
            }
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use crate::errors::BigQueryError;
    use crate::{BigQueryDataset, BigQueryDatasetSummary, BigQueryJob, BigQueryTable};
    use crate::{BigQueryResult, BigQueryTableSummary};
    use gcloud_sdk::google::cloud::bigquery::v2;

    fn assert_unexpected<T: std::fmt::Debug>(what: &str, result: BigQueryResult<T>) {
        match result {
            Err(BigQueryError::SystemError(err)) => {
                assert_eq!(err.public.code, "UNEXPECTED_RESPONSE", "{what}: {err}");
            }
            other => panic!("{what}: expected an unexpected response, got {other:?}"),
        }
    }

    #[test]
    fn a_resource_without_its_reference_is_an_unexpected_response() {
        assert_unexpected("dataset", BigQueryDataset::try_from(v2::Dataset::default()));
        assert_unexpected(
            "listed dataset",
            BigQueryDatasetSummary::try_from(v2::ListFormatDataset::default()),
        );
        assert_unexpected("table", BigQueryTable::try_from(v2::Table::default()));
        assert_unexpected(
            "listed table",
            BigQueryTableSummary::try_from(v2::ListFormatTable::default()),
        );
        assert_unexpected("job", BigQueryJob::try_from(v2::Job::default()));
        assert_unexpected(
            "listed job",
            BigQueryJob::try_from(v2::ListFormatJob::default()),
        );
    }
}
