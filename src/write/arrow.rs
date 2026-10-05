//! Arrow record batches as `AppendRows` payloads: the IPC schema message a connection starts
//! with, and record batch messages sliced to fit one request each.

use crate::errors::BigQueryCodecErrorKind;
use crate::types::error::CodecError;
use crate::BigQueryResult;
use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;
use gcloud_sdk::prost::encoding::encoded_len_varint;

/// The Arrow schema a writer's record batches are serialized against, with its IPC schema
/// message, which BigQuery reads from the first request of a connection only.
#[derive(Debug)]
pub(crate) struct ArrowWriterSchema {
    schema: SchemaRef,
    serialized: Vec<u8>,
}

impl ArrowWriterSchema {
    /// Serializes `schema` as an IPC schema message.
    ///
    /// # Errors
    /// [`BigQueryError::SerializeError`](crate::errors::BigQueryError::SerializeError) of kind
    /// `UnsupportedType` for a schema the IPC writer cannot encode.
    pub(crate) fn new(schema: SchemaRef) -> BigQueryResult<Self> {
        let writer = StreamWriter::try_new(Vec::new(), &schema)
            .map_err(|err| Self::ipc_error(&err).into_serialize())?;
        let serialized = writer.get_ref().clone();
        Ok(Self { schema, serialized })
    }

    /// Whether `batch` has this schema.
    pub(crate) fn describes(&self, batch: &RecordBatch) -> bool {
        let schema = batch.schema_ref();
        std::sync::Arc::ptr_eq(schema, &self.schema) || **schema == *self.schema
    }

    /// The IPC schema message.
    pub(crate) fn serialized(&self) -> &[u8] {
        &self.serialized
    }

    /// The IPC record batch message of `batch`, without the schema message before it.
    ///
    /// Each message is serialized on its own, so that it decodes against the schema message
    /// alone on whichever connection it is sent or resent.
    fn serialize(&self, batch: &RecordBatch) -> Result<Vec<u8>, CodecError> {
        let mut writer =
            StreamWriter::try_new(Vec::new(), &self.schema).map_err(|err| Self::ipc_error(&err))?;
        writer.get_mut().clear();
        writer.write(batch).map_err(|err| Self::ipc_error(&err))?;
        Ok(std::mem::take(writer.get_mut()))
    }

    fn ipc_error(err: &arrow_schema::ArrowError) -> CodecError {
        CodecError::unsupported(format!(
            "the record batch does not serialize as Arrow IPC: {err}"
        ))
    }
}

/// Rows of a caller's record batch serialized for one request.
#[derive(Debug)]
pub(crate) struct RecordBatchSlice {
    pub(crate) row_count: usize,
    pub(crate) serialized: Vec<u8>,
    /// The bytes the slice takes in the request: the serialized batch with its tag and
    /// length.
    pub(crate) cost: usize,
}

/// Slices of one record batch in row order, each as large as fits `capacity` request bytes
/// and `max_rows` rows.
///
/// A slice's size is measured by serializing it: a slice over the capacity is cut into slices
/// of as many rows as its bytes per row say fit the capacity.
pub(crate) struct RecordBatchSlices<'b> {
    batch: &'b RecordBatch,
    schema: &'b ArrowWriterSchema,
    capacity: usize,
    /// The write-order index of the batch's first row, which errors name rows by.
    write_order: u64,
    /// The row ranges still to send, as `(first_row, row_count)`, the next one last.
    pending: Vec<(usize, usize)>,
    /// The IPC bytes serialized so far, slices that did not fit included.
    #[cfg(test)]
    encoded_bytes: usize,
}

impl<'b> RecordBatchSlices<'b> {
    pub(crate) fn new(
        batch: &'b RecordBatch,
        schema: &'b ArrowWriterSchema,
        capacity: usize,
        max_rows: Option<usize>,
        write_order: u64,
    ) -> Self {
        let rows = batch.num_rows();
        let step = max_rows.unwrap_or(rows).max(1);
        let pending = (0..rows)
            .step_by(step)
            .map(|first| (first, step.min(rows - first)))
            .rev()
            .collect();
        Self {
            batch,
            schema,
            capacity,
            write_order,
            pending,
            #[cfg(test)]
            encoded_bytes: 0,
        }
    }
}

impl Iterator for RecordBatchSlices<'_> {
    /// A slice, or the error naming its first row by its index in write order: `RowTooLarge`
    /// for a row that fits no request alone.
    type Item = Result<RecordBatchSlice, CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (first_row, row_count) = self.pending.pop()?;
            let serialized = match self
                .schema
                .serialize(&self.batch.slice(first_row, row_count))
            {
                Ok(serialized) => serialized,
                Err(err) => return Some(Err(err.with_row(self.write_order + first_row as u64))),
            };
            #[cfg(test)]
            {
                self.encoded_bytes += serialized.len();
            }
            let cost = 1 + encoded_len_varint(serialized.len() as u64) + serialized.len();
            if cost <= self.capacity {
                return Some(Ok(RecordBatchSlice {
                    row_count,
                    serialized,
                    cost,
                }));
            }
            if row_count == 1 {
                return Some(Err(CodecError::new(
                    BigQueryCodecErrorKind::RowTooLarge,
                    format!(
                        "the row takes {cost} bytes as Arrow IPC and a request has room for {}",
                        self.capacity
                    ),
                )
                .with_row(self.write_order + first_row as u64)));
            }
            // The measured bytes per row cut the whole range into slices at once, so each row
            // is serialized about once more; a slice that still does not fit is cut again. A
            // sixteenth below the estimate leaves room for what does not grow with the rows.
            let rows_per_slice =
                (row_count as u128 * self.capacity as u128 * 15 / 16 / cost as u128)
                    .clamp(1, row_count as u128 - 1) as usize;
            let slices = (first_row..first_row + row_count)
                .step_by(rows_per_slice)
                .map(|first| (first, rows_per_slice.min(first_row + row_count - first)))
                .rev();
            self.pending.extend(slices);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read::ArrowIpcDecoder;
    use crate::types::testkit::field;
    use crate::write::batch::{append_request, BatchRows, Batcher, RequestTarget};
    use crate::write::descriptor::WritePlan;
    use crate::{BigQueryFieldMode, BigQueryFieldType, BigQueryTableSchema};
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use gcloud_sdk::prost::Message;
    use proptest::prelude::*;
    use std::sync::Arc;

    const STREAM: &str = "projects/p/datasets/shop/tables/orders/streams/_default";

    fn target() -> RequestTarget {
        RequestTarget {
            write_stream: STREAM.to_string(),
            trace_id: None,
            missing_value: None,
        }
    }

    fn batcher(max_request_bytes: usize) -> Batcher {
        let schema = BigQueryTableSchema {
            fields: vec![field(
                "id",
                BigQueryFieldType::Int64,
                BigQueryFieldMode::Required,
            )],
        };
        let plan = Arc::new(WritePlan::new(&schema, false));
        Batcher::new(plan, &target(), max_request_bytes, None)
    }

    /// Orders with a comment of `comment_lengths[i]` bytes each.
    fn orders(comment_lengths: &[usize]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("comment", DataType::Utf8, true),
        ]));
        let ids = Int64Array::from_iter_values(0..comment_lengths.len() as i64);
        let comments: StringArray = comment_lengths
            .iter()
            .map(|&len| Some("c".repeat(len)))
            .collect();
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(comments)])
            .expect("the columns fit the schema")
    }

    proptest! {
        #[test]
        fn slices_fit_their_requests_and_carry_every_row_in_order(
            max_request_bytes in 3_000usize..60_000,
            comment_lengths in proptest::collection::vec(0usize..1_500, 1..400),
            max_rows in proptest::option::of(1usize..50),
        ) {
            let batch = orders(&comment_lengths);
            let schema = Arc::new(
                ArrowWriterSchema::new(batch.schema()).expect("the schema serializes"),
            );
            let batcher = batcher(max_request_bytes);
            let capacity = batcher.arrow_capacity(&schema);
            let mut decoder =
                ArrowIpcDecoder::new(schema.serialized()).expect("the schema decodes");
            let mut next_row = 0;
            for slice in RecordBatchSlices::new(&batch, &schema, capacity, max_rows, 0) {
                let slice = slice.expect("every row fits a request");
                if let Some(max_rows) = max_rows {
                    prop_assert!(slice.row_count <= max_rows);
                }
                let decoded = decoder.decode(&slice.serialized).expect("the slice decodes");
                prop_assert_eq!(&decoded, &batch.slice(next_row, slice.row_count));
                let rows = BatchRows::Arrow {
                    schema: schema.clone(),
                    record_batch: slice.serialized,
                    row_count: slice.row_count as u64,
                };
                let size = append_request(&target(), Some(i64::MAX), &rows, true).encoded_len();
                prop_assert!(size <= max_request_bytes, "{} > {}", size, max_request_bytes);
                next_row += slice.row_count;
            }
            prop_assert_eq!(next_row, batch.num_rows());
        }
    }

    #[test]
    fn a_batch_over_the_cap_is_cut_close_to_the_cap() {
        let batch = orders(&[100; 2_000]);
        let schema = ArrowWriterSchema::new(batch.schema()).expect("the schema serializes");
        let capacity = 50_000;
        let slices: Vec<RecordBatchSlice> =
            RecordBatchSlices::new(&batch, &schema, capacity, None, 0)
                .collect::<Result<_, _>>()
                .expect("every row fits a request");
        let whole = schema
            .serialize(&batch)
            .expect("the batch serializes")
            .len();
        let fewest = whole.div_ceil(capacity);
        assert!(
            slices.len() <= fewest + 1,
            "{} slices for {whole} bytes",
            slices.len()
        );
    }

    #[test]
    fn a_batch_many_times_the_cap_is_serialized_about_twice() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ids = Int64Array::from_iter_values(0..2_000_000);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids)])
            .expect("the column fits the schema");
        let schema = ArrowWriterSchema::new(schema).expect("the schema serializes");
        let mut slices = RecordBatchSlices::new(&batch, &schema, 1_000_000, None, 0);
        let mut rows = 0;
        for slice in slices.by_ref() {
            rows += slice.expect("every row fits a request").row_count;
        }
        assert_eq!(rows, 2_000_000);
        let whole = schema
            .serialize(&batch)
            .expect("the batch serializes")
            .len();
        // The whole batch once to learn its size, then every row once in its slice.
        assert!(
            slices.encoded_bytes <= whole * 21 / 10,
            "{} bytes encoded for a batch of {whole}",
            slices.encoded_bytes
        );
    }

    #[test]
    fn a_row_larger_than_a_request_is_row_too_large_at_its_write_order_index() {
        let batch = orders(&[10, 10, 5_000, 10]);
        let schema = ArrowWriterSchema::new(batch.schema()).expect("the schema serializes");
        let mut slices = RecordBatchSlices::new(&batch, &schema, 2_000, None, 100);
        let mut rows_before = 0;
        let err = loop {
            match slices.next().expect("the large row ends the slices") {
                Ok(slice) => rows_before += slice.row_count,
                Err(err) => break err.into_serialize(),
            }
        };
        assert_eq!(rows_before, 2);
        match err {
            crate::errors::BigQueryError::SerializeError(err) => {
                assert_eq!(err.kind, BigQueryCodecErrorKind::RowTooLarge);
                assert_eq!(err.row, Some(102));
            }
            other => panic!("a row too large is a serialize error, got {other:?}"),
        }
    }
}
