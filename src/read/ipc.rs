use crate::errors::{BigQueryError, BigQueryErrorPublicGenericDetails, BigQuerySystemError};
use crate::BigQueryResult;
use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;

fn ipc_error(message: String) -> BigQueryError {
    BigQueryError::SystemError(BigQuerySystemError::new(
        BigQueryErrorPublicGenericDetails::new("ARROW_IPC".into()),
        message,
    ))
}

/// Decodes the Arrow IPC messages of one read stream or one inline query result: the schema
/// message once, then one record batch message at a time, decompressing LZ4 and ZSTD buffers.
/// It keeps dictionary state between messages, so one decoder serves one stream in order.
pub(crate) struct ArrowIpcDecoder {
    decoder: StreamDecoder,
}

impl ArrowIpcDecoder {
    /// A decoder for the stream whose IPC schema message is `serialized_schema`. The decoder
    /// takes a message without a body as complete only once the next message's bytes arrive,
    /// so a truncated schema shows as an error on the first [`decode`](Self::decode).
    pub(crate) fn new(serialized_schema: &[u8]) -> BigQueryResult<Self> {
        let mut decoder = StreamDecoder::new();
        let mut buffer = Buffer::from(serialized_schema.to_vec());
        while !buffer.is_empty() {
            let batch = decoder
                .decode(&mut buffer)
                .map_err(|e| ipc_error(format!("Failed to decode the Arrow schema: {e}")))?;
            if batch.is_some() {
                return Err(ipc_error(
                    "The Arrow schema message decoded to a record batch".into(),
                ));
            }
        }
        Ok(Self { decoder })
    }

    /// Decodes one IPC record batch message.
    pub(crate) fn decode(&mut self, serialized_record_batch: &[u8]) -> BigQueryResult<RecordBatch> {
        let mut buffer = Buffer::from(serialized_record_batch.to_vec());
        let mut decoded = None;
        while !buffer.is_empty() {
            let batch = self
                .decoder
                .decode(&mut buffer)
                .map_err(|e| ipc_error(format!("Failed to decode an Arrow record batch: {e}")))?;
            match (batch, &decoded) {
                (Some(_), Some(_)) => {
                    return Err(ipc_error(
                        "An Arrow record batch message held more than one batch".into(),
                    ))
                }
                (Some(batch), None) => decoded = Some(batch),
                (None, _) => {}
            }
        }
        decoded.ok_or_else(|| ipc_error("An Arrow record batch message is incomplete".into()))
    }
}
