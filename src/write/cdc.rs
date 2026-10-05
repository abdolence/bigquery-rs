//! CDC writes: upserts and deletes by primary key through the default stream.

use crate::write::connection::FinishKind;
use crate::write::writer::WriterCore;
use crate::{
    BigQueryChange, BigQueryChangeSequenceNumber, BigQueryChangeType, BigQueryDb, BigQueryResult,
    BigQueryStreamingWriteOptions, BigQueryTableRef, BigQueryWriteResponse, BigQueryWriteSummary,
};
use futures::stream::BoxStream;
use serde::Serialize;
use std::borrow::Borrow;
use std::marker::PhantomData;

/// A writer of CDC changes: each row carries `_CHANGE_TYPE` and, optionally,
/// `_CHANGE_SEQUENCE_NUMBER`, and BigQuery applies it to the row with the same primary key.
///
/// The table needs a `PRIMARY KEY ... NOT ENFORCED`. Changes go through the default stream,
/// with its at-least-once delivery; a sequence number makes a change sent twice harmless.
/// Batching, flow control, schema pickup and the response stream work as for
/// [`BigQueryStreamingWriter`](crate::BigQueryStreamingWriter), and so does the warning on a
/// drop without [`finish`](Self::finish).
pub struct BigQueryCdcWriter<T> {
    core: WriterCore,
    _row: PhantomData<fn(&T)>,
}

impl<T: Serialize> BigQueryCdcWriter<T> {
    async fn change(
        &mut self,
        change_type: BigQueryChangeType,
        sequence_number: Option<&BigQueryChangeSequenceNumber>,
        row: &T,
    ) -> BigQueryResult<()> {
        self.core
            .write_with(|encoder, out| {
                encoder.encode_change(row, change_type, sequence_number, out)
            })
            .await
    }

    /// Inserts `row`, or replaces the row with its primary key.
    ///
    /// # Errors
    /// As [`BigQueryStreamingWriter::write`](crate::BigQueryStreamingWriter::write).
    pub async fn upsert(&mut self, row: &T) -> BigQueryResult<()> {
        self.change(BigQueryChangeType::Upsert, None, row).await
    }

    /// Deletes the row with `row`'s primary key; only its key columns matter.
    ///
    /// # Errors
    /// As [`BigQueryStreamingWriter::write`](crate::BigQueryStreamingWriter::write).
    pub async fn delete(&mut self, row: &T) -> BigQueryResult<()> {
        self.change(BigQueryChangeType::Delete, None, row).await
    }

    /// Writes one change with its sequence number, if it has one.
    ///
    /// # Errors
    /// As [`BigQueryStreamingWriter::write`](crate::BigQueryStreamingWriter::write).
    pub async fn write_change<R: Borrow<T>>(
        &mut self,
        change: &BigQueryChange<R>,
    ) -> BigQueryResult<()> {
        self.change(
            change.change_type,
            change.sequence_number.as_ref(),
            change.row.borrow(),
        )
        .await
    }

    /// As [`BigQueryStreamingWriter::flush`](crate::BigQueryStreamingWriter::flush).
    pub async fn flush(&mut self) -> BigQueryResult<()> {
        self.core.flush().await.map(|_| ())
    }

    /// Flushes, waits for every acknowledgement and closes the connection.
    ///
    /// # Errors
    /// The writer's failure, if it failed for good.
    pub async fn finish(mut self) -> BigQueryResult<BigQueryWriteSummary> {
        Ok(self.core.finish(FinishKind::Close).await?.summary)
    }
}

impl BigQueryDb {
    /// Opens a CDC writer on `table`.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`](crate::errors::BigQueryError::InvalidParametersError)
    /// for any mode other than [`Default`](crate::BigQueryWriteMode::Default): CDC is
    /// default-stream only. Otherwise the failure to open the stream.
    pub async fn create_cdc_writer<'b, T: Serialize>(
        &self,
        table: impl Into<BigQueryTableRef>,
        options: BigQueryStreamingWriteOptions,
    ) -> BigQueryResult<(
        BigQueryCdcWriter<T>,
        BoxStream<'b, BigQueryResult<BigQueryWriteResponse>>,
    )> {
        let (core, responses) = WriterCore::open(self, table.into(), options, true).await?;
        Ok((
            BigQueryCdcWriter {
                core,
                _row: PhantomData,
            },
            responses,
        ))
    }
}
