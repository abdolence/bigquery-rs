use crate::errors::{BigQueryCodecErrorKind, BigQueryError};
use crate::types::error::CodecError;
use crate::BigQueryTableRef;
use rsb_derive::Builder;
use std::str::FromStr;
use std::time::Duration;

/// What an insert or a streaming writer writes to and how.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct BigQueryInsertParams {
    /// The table to write.
    pub table: BigQueryTableRef,
    /// How the writer opens its stream and batches its rows.
    #[default = "BigQueryStreamingWriteOptions::new()"]
    pub options: BigQueryStreamingWriteOptions,
}

/// How a streaming writer opens its stream, batches rows and bounds what is in flight.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct BigQueryStreamingWriteOptions {
    /// Which write stream the rows go through, and so the delivery guarantee.
    #[default = "BigQueryWriteMode::Default"]
    pub mode: BigQueryWriteMode,
    /// The largest append request in bytes; it cannot be set above the default, since BigQuery
    /// ends the whole connection on a request over its limit.
    #[default = "19_000_000"]
    pub max_request_bytes: usize,
    /// Sends a batch once it holds this many rows.
    pub max_batch_rows: Option<usize>,
    /// Sends a batch once this long has passed since its first row.
    #[default = "Duration::from_millis(100)"]
    pub max_batch_delay: Duration,
    /// The most unacknowledged requests; `write()` waits beyond it.
    #[default = "8"]
    pub max_inflight_requests: usize,
    /// The most unacknowledged bytes; `write()` waits beyond it.
    #[default = "64 * 1024 * 1024"]
    pub max_inflight_bytes: usize,
    /// The shortest time between two schema refreshes caused by fields with no column.
    #[default = "Duration::from_secs(1)"]
    pub schema_refresh_interval: Duration,
    /// What BigQuery stores for a field a row leaves out.
    pub missing_value: Option<BigQueryMissingValue>,
    /// The `trace_id` sent with the stream, for BigQuery's own diagnostics.
    pub trace_id: Option<String>,
}

/// The write stream a writer uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BigQueryWriteMode {
    /// The table's `_default` stream: rows are visible when acknowledged, at least once.
    Default,
    /// A committed stream with offsets: each acknowledged row exactly once.
    Committed,
    /// A pending stream: all rows become visible at the commit, or none do.
    Pending,
}

/// What BigQuery stores for a field that a row leaves out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BigQueryMissingValue {
    /// NULL.
    Null,
    /// The column's default value expression.
    DefaultValue,
}

/// The outcome of one acknowledged batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryWriteResponse {
    /// The batch's index in send order.
    pub batch_index: u64,
    /// The write-order index of the batch's first row.
    pub first_row: u64,
    /// The number of rows in the batch.
    pub row_count: u64,
    /// The stream offset the batch was written at; `None` on the default stream.
    pub offset: Option<i64>,
}

/// What a finished writer wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryWriteSummary {
    /// Rows acknowledged as written.
    pub rows_written: u64,
    /// Rows in batches that failed.
    pub rows_failed: u64,
    /// Batches sent.
    pub batches: u64,
    /// The write stream's name; `None` for the default stream.
    pub stream: Option<String>,
    /// When a pending stream was committed.
    pub commit_time: Option<jiff::Timestamp>,
}

/// A finalized pending stream, ready for a commit together with other streams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryFinalizedStream {
    /// The table the stream writes.
    pub table: BigQueryTableRef,
    /// The write stream's name.
    pub name: String,
    /// The rows the stream holds, as `FinalizeWriteStream` reported them.
    pub row_count: i64,
}

/// What a CDC change does to the row with its primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BigQueryChangeType {
    /// Inserts the row or replaces the one with the same key.
    Upsert,
    /// Deletes the row with the same key.
    Delete,
}

/// A CDC `_CHANGE_SEQUENCE_NUMBER`, which orders changes to one key.
///
/// One to four `/`-separated sections of one to sixteen hex digits, as BigQuery documents
/// `_CHANGE_SEQUENCE_NUMBER` (unverified). `From<u64>` writes one section.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BigQueryChangeSequenceNumber(String);

impl From<u64> for BigQueryChangeSequenceNumber {
    fn from(n: u64) -> Self {
        Self(format!("{n:X}"))
    }
}

impl BigQueryChangeSequenceNumber {
    /// The text written to `_CHANGE_SEQUENCE_NUMBER`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for BigQueryChangeSequenceNumber {
    type Err = BigQueryError;

    /// Parses one to four `/`-separated sections of one to sixteen hex digits.
    ///
    /// # Errors
    /// [`BigQueryError::SerializeError`] with kind
    /// [`InvalidText`](crate::errors::BigQueryCodecErrorKind::InvalidText) for any other text.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let sections = s.split('/');
        let valid = s.split('/').count() <= 4
            && sections.into_iter().all(|section| {
                (1..=16).contains(&section.len()) && section.bytes().all(|b| b.is_ascii_hexdigit())
            });
        if !valid {
            return Err(CodecError::new(
                BigQueryCodecErrorKind::InvalidText,
                format!(
                    "invalid _CHANGE_SEQUENCE_NUMBER `{s}`, expected one to four `/`-separated \
                     sections of one to sixteen hex digits"
                ),
            )
            .into_serialize());
        }
        Ok(Self(s.to_string()))
    }
}

/// One CDC change of a row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryChange<T> {
    /// Upsert or delete.
    pub change_type: BigQueryChangeType,
    /// Orders this change against others to the same key; `None` leaves the order to arrival.
    pub sequence_number: Option<BigQueryChangeSequenceNumber>,
    /// The row; for a delete only its primary key columns matter.
    pub row: T,
}

#[cfg(test)]
mod tests;
