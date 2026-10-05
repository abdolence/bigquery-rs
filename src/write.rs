mod types;
pub use types::*;

pub(crate) mod arrow;
pub(crate) mod batch;
mod cdc;
pub(crate) mod connection;
pub(crate) mod descriptor;
pub(crate) mod encoder;
mod support;
mod writer;

pub use cdc::BigQueryCdcWriter;
pub use writer::BigQueryStreamingWriter;

/// The row encoder alone, for `benches/write_codec.rs`; not part of the API.
#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub struct BigQueryWriteCodecBench {
    encoder: encoder::Encoder,
}

#[cfg(feature = "bench-internals")]
#[doc(hidden)]
impl BigQueryWriteCodecBench {
    pub fn new(schema: &crate::BigQueryTableSchema) -> Self {
        let plan = std::sync::Arc::new(descriptor::WritePlan::new(schema, false));
        Self {
            encoder: encoder::Encoder::new(plan),
        }
    }

    /// Appends `row`'s protobuf encoding to `out`.
    pub fn encode<T: serde::Serialize>(
        &mut self,
        row: &T,
        out: &mut Vec<u8>,
    ) -> crate::BigQueryResult<()> {
        self.encoder
            .encode(row, out)
            .map_err(crate::types::error::CodecError::into_serialize)
    }
}
