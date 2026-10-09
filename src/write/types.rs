use crate::errors::BigQueryError;
use crate::types::error::CodecError;
use crate::write::batch::MAX_REQUEST_BYTES;
use crate::BigQueryInstant;
use crate::{BigQueryResult, BigQueryTableRef};
use rsb_derive::Builder;
use std::fmt::{Display, Formatter};
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
    #[default = "MAX_REQUEST_BYTES"]
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
    pub trace_id: Option<BigQueryTraceId>,
}

/// The `trace_id` a writer sends with its stream, which BigQuery keeps for its own diagnostics;
/// Google suggests the client's name and version.
///
/// Checked only for being non-empty, since an empty one means none.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct BigQueryTraceId(String);

impl BigQueryTraceId {
    /// Checks `id` and wraps it.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for the field `trace_id` if it is empty.
    pub fn new(id: impl Into<String>) -> BigQueryResult<Self> {
        let id = id.into();
        if id.is_empty() {
            return Err(BigQueryError::invalid_parameters(
                "trace_id",
                "must not be empty",
            ));
        }
        Ok(Self(id))
    }

    /// The ID.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for BigQueryTraceId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<&str> for BigQueryTraceId {
    type Error = BigQueryError;

    fn try_from(id: &str) -> Result<Self, Self::Error> {
        Self::new(id)
    }
}

impl TryFrom<String> for BigQueryTraceId {
    type Error = BigQueryError;

    fn try_from(id: String) -> Result<Self, Self::Error> {
        Self::new(id)
    }
}

impl FromStr for BigQueryTraceId {
    type Err = BigQueryError;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        Self::new(id)
    }
}

/// The name of a write stream, as `CreateWriteStream` returned it:
/// `projects/{p}/datasets/{d}/tables/{t}/streams/{id}`.
///
/// Only BigQuery makes one, so there is no public constructor; a stream is reachable only
/// through the writer that created it.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct BigQueryWriteStreamName(String);

impl BigQueryWriteStreamName {
    pub(crate) fn reported(name: String) -> Self {
        Self(name)
    }

    /// The name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for BigQueryWriteStreamName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The write stream a writer uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BigQueryWriteMode {
    /// The table's `_default` stream: rows are visible when acknowledged, at least once.
    Default,
    /// A committed stream with offsets: each acknowledged row exactly once.
    Committed,
    /// A pending stream: all rows become visible at the commit, or none do.
    Pending,
    /// A buffered stream with offsets: each acknowledged row exactly once, readable only up
    /// to the offset the writer last flushed with
    /// [`flush_rows`](crate::BigQueryStreamingWriter::flush_rows) or
    /// [`flush_rows_to`](crate::BigQueryStreamingWriter::flush_rows_to).
    Buffered,
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
    /// Bytes of every `AppendRows` request sent, resent ones included, before gRPC framing.
    pub bytes_sent: u64,
    /// The write stream's name; `None` for the default stream.
    pub stream: Option<BigQueryWriteStreamName>,
    /// When a pending stream was committed.
    pub commit_time: Option<BigQueryInstant>,
}

/// A finalized pending stream, ready for a commit together with other streams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryFinalizedStream {
    /// The table the stream writes.
    pub table: BigQueryTableRef,
    /// The write stream's name.
    pub name: BigQueryWriteStreamName,
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

impl BigQueryChangeType {
    /// The value of the `_CHANGE_TYPE` pseudo-column, as in `UPSERT`.
    pub(crate) fn name(self) -> &'static str {
        match self {
            BigQueryChangeType::Upsert => "UPSERT",
            BigQueryChangeType::Delete => "DELETE",
        }
    }

    /// The change `text` names, as [`FromStr`] reads it, failing with the codec error a decoder
    /// places at its field.
    pub(crate) fn from_text(text: &str) -> Result<Self, CodecError> {
        match text {
            "UPSERT" => Ok(BigQueryChangeType::Upsert),
            "DELETE" => Ok(BigQueryChangeType::Delete),
            _ => Err(CodecError::invalid_text(format!(
                "{text:?} is not UPSERT or DELETE"
            ))),
        }
    }
}

impl FromStr for BigQueryChangeType {
    type Err = BigQueryError;

    /// The change a `_CHANGE_TYPE` value names, exactly as BigQuery spells it: `UPSERT` or
    /// `DELETE`.
    ///
    /// # Errors
    /// [`BigQueryError::SerializeError`] with kind
    /// [`InvalidText`](crate::errors::BigQueryCodecErrorKind::InvalidText) for any other text.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_text(s).map_err(CodecError::into_serialize)
    }
}

/// A CDC `_CHANGE_SEQUENCE_NUMBER`, which orders changes to one key. `From<u64>` writes it in
/// hex; any other text is sent as it is, and BigQuery checks its form.
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

    /// Takes the text as it is. An empty text is refused, since it would be sent as no
    /// sequence number at all; leave [`BigQueryChange::sequence_number`] `None` for that.
    ///
    /// # Errors
    /// [`BigQueryError::SerializeError`] with kind
    /// [`InvalidText`](crate::errors::BigQueryCodecErrorKind::InvalidText) for an empty text.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(
                CodecError::invalid_text("an empty _CHANGE_SEQUENCE_NUMBER").into_serialize()
            );
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
mod tests {
    use super::*;
    use crate::errors::BigQueryCodecErrorKind;

    #[test]
    fn a_sequence_number_is_sent_as_written_and_only_empty_is_refused() {
        for text in ["1F", "A/B/C/D", "g", "1/2/3/4/5", "11111111111111111"] {
            let parsed: BigQueryChangeSequenceNumber = text.parse().expect(text);
            assert_eq!(parsed.as_str(), text);
        }
        match "".parse::<BigQueryChangeSequenceNumber>() {
            Err(BigQueryError::SerializeError(err)) => {
                assert_eq!(err.kind, BigQueryCodecErrorKind::InvalidText);
            }
            other => panic!("an empty sequence number must be InvalidText, got {other:?}"),
        }
        assert_eq!(BigQueryChangeSequenceNumber::from(255).as_str(), "FF");
    }

    #[test]
    fn an_empty_trace_id_is_refused() {
        match BigQueryTraceId::new("") {
            Err(BigQueryError::InvalidParametersError(err)) => {
                assert_eq!(err.public.field, "trace_id")
            }
            other => panic!("expected an invalid trace ID, got {other:?}"),
        }
        assert_eq!(
            BigQueryTraceId::new("app:1.0")
                .expect("a trace ID")
                .as_str(),
            "app:1.0"
        );
    }
}
