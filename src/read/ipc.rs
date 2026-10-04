use crate::errors::BigQueryError;
use crate::BigQueryResult;
use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;

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
            let batch = decoder.decode(&mut buffer).map_err(|e| {
                BigQueryError::system(
                    "ARROW_IPC",
                    format!("Failed to decode the Arrow schema: {e}"),
                )
            })?;
            if batch.is_some() {
                return Err(BigQueryError::system(
                    "ARROW_IPC",
                    "The Arrow schema message decoded to a record batch",
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
            let batch = self.decoder.decode(&mut buffer).map_err(|e| {
                BigQueryError::system(
                    "ARROW_IPC",
                    format!("Failed to decode an Arrow record batch: {e}"),
                )
            })?;
            match (batch, &decoded) {
                (Some(_), Some(_)) => {
                    return Err(BigQueryError::system(
                        "ARROW_IPC",
                        "An Arrow record batch message held more than one batch",
                    ))
                }
                (Some(batch), None) => decoded = Some(batch),
                (None, _) => {}
            }
        }
        decoded.ok_or_else(|| {
            BigQueryError::system("ARROW_IPC", "An Arrow record batch message is incomplete")
        })
    }
}
