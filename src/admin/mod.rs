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

use crate::errors::{BigQueryCodecErrorKind, BigQueryError};
use crate::types::error::CodecError;
use crate::BigQueryInstant;
use crate::BigQueryResult;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use std::future::Future;
use std::time::Duration;
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

/// A v2 timestamp in milliseconds since the epoch; 0 is unset.
pub(crate) fn timestamp_ms(field: &str, ms: i64) -> BigQueryResult<Option<BigQueryInstant>> {
    if ms == 0 {
        return Ok(None);
    }
    BigQueryInstant::from_millisecond(ms)
        .map(Some)
        .map_err(|err| out_of_range(field, format!("{ms} ms is not a timestamp: {err}")))
}

/// A v2 duration in milliseconds; unset or 0 is no duration.
pub(crate) fn duration_ms(field: &str, ms: Option<i64>) -> BigQueryResult<Option<Duration>> {
    match ms.filter(|ms| *ms != 0) {
        None => Ok(None),
        Some(ms) => u64::try_from(ms)
            .map(|ms| Some(Duration::from_millis(ms)))
            .map_err(|_| out_of_range(field, format!("{ms} ms is a negative duration"))),
    }
}

fn out_of_range(field: &str, message: String) -> BigQueryError {
    CodecError::new(BigQueryCodecErrorKind::OutOfRange, message)
        .at_field(field)
        .into_deserialize()
}

/// An empty optional string in a v2 message is unset.
pub(crate) fn non_empty(s: String) -> Option<String> {
    (!s.is_empty()).then_some(s)
}
