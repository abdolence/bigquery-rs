//! Rows grouped into append requests that never exceed the request cap.
//!
//! BigQuery accepted an append of 20,488,556 bytes and rejected one of 20,989,006 with a bare
//! `InvalidArgument` that ends the whole connection, so a batch is sized exactly before it is
//! sent and never split after a rejection.

use crate::errors::BigQueryCodecErrorKind;
use crate::types::error::CodecError;
use crate::write::descriptor::WritePlan;
use crate::write::encoder::varint_len;
use crate::BigQueryMissingValue;
use gcloud_sdk::google::cloud::bigquery::storage::v1::append_rows_request::{
    self, MissingValueInterpretation, ProtoData,
};
use gcloud_sdk::google::cloud::bigquery::storage::v1::{AppendRowsRequest, ProtoRows, ProtoSchema};
use gcloud_sdk::prost::Message;
use std::sync::Arc;
use tokio::time::Instant;

/// The largest `max_request_bytes` a writer accepts.
pub(crate) const MAX_REQUEST_BYTES: usize = 19_000_000;

/// The two length prefixes around the rows, of `ProtoData` and of `ProtoRows`, take one byte
/// each when empty and at most four below 2^28 bytes, which is far above the cap.
const NESTED_LENGTH_SLACK: usize = 2 * 3;

/// What every request of one writer carries besides its rows and offset.
#[derive(Debug, Clone)]
pub(crate) struct RequestTarget {
    pub(crate) write_stream: String,
    pub(crate) trace_id: Option<String>,
    pub(crate) missing_value: Option<BigQueryMissingValue>,
}

/// Builds one append request. `plan` is the writer schema to send with it, `None` when the
/// connection already has it.
pub(crate) fn append_request(
    target: &RequestTarget,
    offset: Option<i64>,
    plan: Option<&WritePlan>,
    rows: Vec<Vec<u8>>,
) -> AppendRowsRequest {
    AppendRowsRequest {
        // Every request names the stream: after an in-band schema switch BigQuery treats the
        // connection as multiplexed and rejects requests without it.
        write_stream: target.write_stream.clone(),
        offset,
        trace_id: target.trace_id.clone().unwrap_or_default(),
        default_missing_value_interpretation: match target.missing_value {
            None => MissingValueInterpretation::Unspecified,
            Some(BigQueryMissingValue::Null) => MissingValueInterpretation::NullValue,
            Some(BigQueryMissingValue::DefaultValue) => MissingValueInterpretation::DefaultValue,
        }
        .into(),
        rows: Some(append_rows_request::Rows::ProtoRows(ProtoData {
            writer_schema: plan.map(|plan| ProtoSchema {
                proto_descriptor: Some(plan.descriptor().clone()),
            }),
            rows: Some(ProtoRows {
                serialized_rows: rows,
            }),
        })),
        ..Default::default()
    }
}

impl WritePlan {
    /// The bytes of a request that are not rows, with the writer schema always counted: a batch
    /// can become the first request of a new connection when it is resent after a reconnect.
    fn request_overhead(&self, target: &RequestTarget) -> usize {
        append_request(target, Some(i64::MAX), Some(self), Vec::new()).encoded_len()
            + NESTED_LENGTH_SLACK
    }
}

/// A sealed batch: rows encoded against one plan, the unit of sending, acknowledging and
/// resending.
#[derive(Debug)]
pub(crate) struct Batch {
    pub(crate) index: u64,
    pub(crate) first_row: u64,
    pub(crate) rows: Vec<Vec<u8>>,
    /// The bytes the rows take in the request, framing included.
    pub(crate) bytes: usize,
    pub(crate) plan: Arc<WritePlan>,
    /// How many times it was sent.
    pub(crate) attempts: usize,
}

impl Batch {
    pub(crate) fn row_count(&self) -> u64 {
        self.rows.len() as u64
    }
}

#[derive(Debug)]
struct OpenBatch {
    first_row: u64,
    rows: Vec<Vec<u8>>,
    bytes: usize,
    since: Instant,
}

/// Groups encoded rows into batches that fit one request under `max_request_bytes`.
#[derive(Debug)]
pub(crate) struct Batcher {
    plan: Arc<WritePlan>,
    target: RequestTarget,
    max_request_bytes: usize,
    max_rows: Option<usize>,
    overhead: usize,
    open: Option<OpenBatch>,
    next_index: u64,
    next_row: u64,
}

impl Batcher {
    pub(crate) fn new(
        plan: Arc<WritePlan>,
        target: &RequestTarget,
        max_request_bytes: usize,
        max_rows: Option<usize>,
    ) -> Self {
        let overhead = plan.request_overhead(target);
        Batcher {
            plan,
            target: target.clone(),
            max_request_bytes,
            max_rows,
            overhead,
            open: None,
            next_index: 0,
            next_row: 0,
        }
    }

    /// The bytes a request reserves for everything but its rows.
    #[cfg(test)]
    pub(crate) fn overhead(&self) -> usize {
        self.overhead
    }

    /// The bytes left for rows in one request.
    pub(crate) fn capacity(&self) -> usize {
        self.max_request_bytes.saturating_sub(self.overhead)
    }

    /// The write-order index the next row gets.
    pub(crate) fn next_row(&self) -> u64 {
        self.next_row
    }

    /// What a row of `len` encoded bytes takes in a request: its tag, its length and itself.
    ///
    /// # Errors
    /// `RowTooLarge` when the row cannot fit any request alone.
    pub(crate) fn cost(&self, len: usize) -> Result<usize, CodecError> {
        let cost = 1 + varint_len(len as u64) + len;
        if cost > self.capacity() {
            return Err(CodecError::new(
                BigQueryCodecErrorKind::RowTooLarge,
                format!(
                    "the row takes {cost} bytes and a request has room for {} rows bytes under \
                     max_request_bytes = {}",
                    self.capacity(),
                    self.max_request_bytes
                ),
            ));
        }
        Ok(cost)
    }

    /// Whether the open batch, if any, can take one more row of `cost`.
    pub(crate) fn fits(&self, cost: usize) -> bool {
        match &self.open {
            None => true,
            Some(open) => {
                open.bytes + cost <= self.capacity()
                    && self.max_rows.is_none_or(|max| open.rows.len() < max)
            }
        }
    }

    /// Whether the open batch has reached `max_batch_rows`.
    pub(crate) fn is_full(&self) -> bool {
        match (&self.open, self.max_rows) {
            (Some(open), Some(max)) => open.rows.len() >= max,
            _ => false,
        }
    }

    /// Adds a row to the open batch, opening one at `now` if there is none. Returns whether
    /// it opened a batch. The caller checked [`fits`](Self::fits) first.
    pub(crate) fn push(&mut self, row: Vec<u8>, cost: usize, now: Instant) -> bool {
        let opened = self.open.is_none();
        let next_row = self.next_row;
        let open = self.open.get_or_insert_with(|| OpenBatch {
            first_row: next_row,
            rows: Vec::new(),
            bytes: 0,
            since: now,
        });
        open.rows.push(row);
        open.bytes += cost;
        self.next_row += 1;
        opened
    }

    /// When the open batch got its first row.
    pub(crate) fn open_since(&self) -> Option<Instant> {
        self.open.as_ref().map(|open| open.since)
    }

    /// The bytes of the open batch's rows.
    pub(crate) fn open_bytes(&self) -> Option<usize> {
        self.open.as_ref().map(|open| open.bytes)
    }

    /// Closes the open batch, if any.
    pub(crate) fn seal(&mut self) -> Option<Batch> {
        let open = self.open.take()?;
        let index = self.next_index;
        self.next_index += 1;
        Some(Batch {
            index,
            first_row: open.first_row,
            rows: open.rows,
            bytes: open.bytes,
            plan: self.plan.clone(),
            attempts: 0,
        })
    }

    /// The plan for the rows from now on. The open batch must be sealed first, since a batch
    /// holds rows of one plan only.
    pub(crate) fn set_plan(&mut self, plan: Arc<WritePlan>) {
        debug_assert!(self.open.is_none(), "a batch holds rows of one plan only");
        self.overhead = plan.request_overhead(&self.target);
        self.plan = plan;
    }
}

#[cfg(test)]
mod tests;
