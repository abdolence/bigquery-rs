//! Table reads through the Storage Read API.
//!
//! One read opens one session, and each of its streams runs on its own task: `ReadRows`, the
//! Arrow IPC decode and, for typed reads, the row decode. The streams meet in one bounded
//! channel, so rows from different streams arrive in no particular order. A stream that fails
//! with a retryable error is resumed at its row offset; one that fails for good ends the whole
//! read after its error.

mod params;
pub use params::*;

mod decoder;
mod ipc;
mod keys;
mod projection;
mod session;
mod stream;
mod support;

pub(crate) use decoder::decode_rows;
pub use decoder::BigQueryBatchRows;
pub(crate) use ipc::ArrowIpcDecoder;
pub(crate) use support::skip_failed_rows;

use crate::{BigQueryDb, BigQueryResult};
use arrow_array::RecordBatch;
use futures::stream::BoxStream;
use serde::de::DeserializeOwned;
use session::Projection;
use tracing::field::Empty;

impl BigQueryReadParams {
    /// The span of one read terminal call.
    fn span(&self) -> tracing::Span {
        tracing::debug_span!(
            "BigQuery Read",
            "/bigquery/table" = %self.table,
            "/bigquery/streams" = Empty,
            "/bigquery/estimated_bytes_scanned" = Empty,
            "/bigquery/estimated_rows" = Empty,
            "/bigquery/rows_read" = Empty,
            "/bigquery/bytes_read" = Empty,
            "/bigquery/throttle_percent" = Empty,
        )
    }
}

impl BigQueryDb {
    /// Reads `params.table` as the record batches BigQuery sends, after IPC decode and
    /// decompression. Without `selected_fields` every column is read.
    pub(crate) async fn read_table_batches<'b>(
        &self,
        params: BigQueryReadParams,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<RecordBatch>>> {
        let span = params.span();
        let session = self
            .open_read_session(&params, Projection::All, &span)
            .await?;
        session.record(&span);
        Ok(self
            .start_read_streams(session, &span, |batch, _| batch)
            .into_batches())
    }

    /// Reads `params.table` decoded into `T`, each row on its stream's task. Without
    /// `selected_fields` the read selects the columns that `T`'s top-level fields name, when `T`
    /// is a plain struct, and every column otherwise.
    pub(crate) async fn read_table_rows<'b, T>(
        &self,
        params: BigQueryReadParams,
    ) -> BigQueryResult<BoxStream<'b, BigQueryResult<T>>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let span = params.span();
        let projection = match projection::struct_fields::<T>() {
            Some(fields) => Projection::Auto(fields),
            None => Projection::All,
        };
        let session = self.open_read_session(&params, projection, &span).await?;
        session.record(&span);
        Ok(self
            .start_read_streams(session, &span, |batch, first_row| {
                decode_rows::<T>(&batch, first_row)
            })
            .into_rows())
    }
}

#[cfg(test)]
mod tests;
