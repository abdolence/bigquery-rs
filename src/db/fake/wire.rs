//! The wire forms the fake servers answer in: Arrow IPC messages, as query results and read
//! streams carry them, and the `StorageError` detail of a Storage Write status.

use arrow_array::RecordBatch;
use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow_ipc::CompressionType;
use arrow_schema::{ArrowError, Schema};
use gcloud_sdk::google::cloud::bigquery::storage::v1 as storage;
use gcloud_sdk::google::cloud::bigquery::storage::v1::storage_error::StorageErrorCode;
use gcloud_sdk::google::cloud::bigquery::storage::v1::StorageError;
use gcloud_sdk::google::cloud::bigquery::v2;
use gcloud_sdk::google::rpc::Status;
use gcloud_sdk::prost::Message;
use gcloud_sdk::tonic::Code;

/// The buffer compression of IPC messages, as a read session or a query asked for it.
/// `None` sends the buffers as they are.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct IpcCompression(Option<CompressionType>);

/// The compression a read session's Arrow options ask for.
impl From<storage::arrow_serialization_options::CompressionCodec> for IpcCompression {
    fn from(codec: storage::arrow_serialization_options::CompressionCodec) -> Self {
        use storage::arrow_serialization_options::CompressionCodec;
        Self(match codec {
            CompressionCodec::Lz4Frame => Some(CompressionType::LZ4_FRAME),
            CompressionCodec::Zstd => Some(CompressionType::ZSTD),
            CompressionCodec::CompressionUnspecified => None,
        })
    }
}

/// The compression a query's Arrow results options ask for.
impl From<v2::arrow_serialization_options::CompressionCodec> for IpcCompression {
    fn from(codec: v2::arrow_serialization_options::CompressionCodec) -> Self {
        use v2::arrow_serialization_options::CompressionCodec;
        Self(match codec {
            CompressionCodec::Lz4Frame => Some(CompressionType::LZ4_FRAME),
            CompressionCodec::Zstd => Some(CompressionType::ZSTD),
            CompressionCodec::CompressionUnspecified => None,
        })
    }
}

/// The messages of one Arrow IPC stream, each on its own as BigQuery sends them: the schema
/// once, then each record batch.
pub(crate) struct IpcMessages {
    pub schema: Vec<u8>,
    pub batches: Vec<Vec<u8>>,
}

impl IpcMessages {
    /// Encodes `batches`, each of which has `schema`, with their buffers compressed as
    /// `compression` says.
    ///
    /// # Errors
    /// The writer's error for a batch whose columns do not match `schema`.
    pub(crate) fn encode(
        schema: &Schema,
        batches: &[RecordBatch],
        compression: IpcCompression,
    ) -> Result<Self, ArrowError> {
        let options = IpcWriteOptions::default().try_with_compression(compression.0)?;
        let mut writer = StreamWriter::try_new_with_options(Vec::new(), schema, options)?;
        let schema = writer.get_ref().clone();
        let batches = batches
            .iter()
            .map(|batch| {
                let start = writer.get_ref().len();
                writer.write(batch)?;
                Ok(writer.get_ref()[start..].to_vec())
            })
            .collect::<Result<_, ArrowError>>()?;
        Ok(Self { schema, batches })
    }
}

/// A Storage Write status under `code`, carrying a `StorageError` of `storage` on `entity`, the
/// write stream or table it is about, as the writer reads it.
#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "bigquery::testing answers in-band AppendRows errors with it"
    )
)]
pub(crate) fn storage_error_status(
    code: Code,
    storage: StorageErrorCode,
    entity: &str,
    message: &str,
) -> Status {
    Status {
        code: code as i32,
        message: message.into(),
        details: vec![gcloud_sdk::prost_types::Any {
            type_url: "type.googleapis.com/google.cloud.bigquery.storage.v1.StorageError".into(),
            value: StorageError {
                code: storage.into(),
                entity: entity.into(),
                error_message: message.into(),
            }
            .encode_to_vec(),
        }],
    }
}
