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
#[allow(unused_imports, reason = "queries decode their inline Arrow results with it")]
pub(crate) use ipc::ArrowIpcDecoder;

use crate::{BigQueryDb, BigQueryResult};
use arrow_array::RecordBatch;
use futures::stream::BoxStream;
use serde::de::DeserializeOwned;
use session::{open_session, Projection};
use tracing::field::Empty;

fn read_span(params: &BigQueryReadParams) -> tracing::Span {
    tracing::debug_span!(
        "BigQuery Read",
        "/bigquery/table" = %params.table,
        "/bigquery/streams" = Empty,
    )
}

/// Reads `params.table` as the record batches BigQuery sends, after IPC decode and
/// decompression. Without `selected_fields` every column is read.
pub(crate) async fn read_table_batches<'b>(
    db: &BigQueryDb,
    params: BigQueryReadParams,
) -> BigQueryResult<BoxStream<'b, BigQueryResult<RecordBatch>>> {
    let span = read_span(&params);
    let session = open_session(db, &params, Projection::All, &span).await?;
    span.record("/bigquery/streams", session.streams.len());
    Ok(stream::start_streams(db, session, &span, |batch, _| batch).into_batches())
}

/// Reads `params.table` decoded into `T`, each row on its stream's task. Without
/// `selected_fields` the read selects the columns that `T`'s top-level fields name, when `T`
/// is a plain struct, and every column otherwise.
pub(crate) async fn read_table_rows<'b, T>(
    db: &BigQueryDb,
    params: BigQueryReadParams,
) -> BigQueryResult<BoxStream<'b, BigQueryResult<T>>>
where
    T: DeserializeOwned + Send + 'static,
{
    let span = read_span(&params);
    let projection = match projection::struct_fields::<T>() {
        Some(fields) => Projection::Auto(fields),
        None => Projection::All,
    };
    let session = open_session(db, &params, projection, &span).await?;
    span.record("/bigquery/streams", session.streams.len());
    Ok(
        stream::start_streams(db, session, &span, |batch, first_row| {
            decode_rows::<T>(&batch, first_row)
        })
        .into_rows(),
    )
}

/// Decodes every row of `batch` into `T` and calls `f` with each result. Not part of the API:
/// it exists so that the codec benchmark can time the decoder, which benchmarks cannot reach
/// otherwise.
#[doc(hidden)]
pub fn __bench_decode_each<T, F>(batch: &RecordBatch, f: F)
where
    T: DeserializeOwned,
    F: FnMut(BigQueryResult<T>),
{
    decoder::decode_each(batch, 0, f)
}
