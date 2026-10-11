//! The writer chain: batches in a table's engine Arrow types become Parquet data files of the
//! table, one file per partition a batch touches, more when a file rolls over at the target
//! size.
//!
//! Every file name starts with the UUID of the writer that wrote it, a fresh one per
//! [`TableWriter`], so no two writers ever produce the same path. That is what keeps an append
//! from adding a path the table already holds, since the engine's appends skip iceberg's
//! duplicate check.

use crate::catalog::LoadedTable;
use crate::error::EngineError;
use arrow::array::RecordBatch;
use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::spec::{DataFile, DataFileFormat, PartitionSpecRef, SchemaRef};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

type FileWriterBuilder = RollingFileWriterBuilder<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

/// The data files a write produced, for the table identity they were written for.
#[derive(Debug, Clone)]
pub(crate) struct WrittenFiles {
    pub files: Vec<DataFile>,
    /// The UUID of the table identity the files were written for: a commit lands them only
    /// while the table under the name is still that identity.
    pub table_uuid: Uuid,
    pub rows: u64,
}

/// Writes batches into data files of one table identity, as its current schema and partition
/// spec define them.
pub(crate) struct TableWriter {
    builder: FileWriterBuilder,
    schema: SchemaRef,
    spec: PartitionSpecRef,
    table_uuid: Uuid,
}

impl TableWriter {
    /// A writer of new files of `table`, named after a fresh writer UUID.
    pub(crate) fn new(table: &LoadedTable) -> Result<Self, EngineError> {
        let metadata = table.metadata();
        let schema = metadata.current_schema().clone();
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build();
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            ParquetWriterBuilder::new(properties, schema.clone()),
            table.table.file_io().clone(),
            DefaultLocationGenerator::new(metadata)?,
            DefaultFileNameGenerator::new(
                Uuid::new_v4().to_string(),
                None,
                DataFileFormat::Parquet,
            ),
        );
        Ok(Self {
            builder: rolling,
            schema,
            spec: metadata.default_partition_spec().clone(),
            table_uuid: metadata.uuid(),
        })
    }

    /// Writes `batches`, whose schema is the table's engine Arrow schema with its field ids,
    /// and returns every file written. Nothing references the files until a commit adds them.
    pub(crate) async fn write(
        &self,
        batches: impl IntoIterator<Item = RecordBatch>,
    ) -> Result<WrittenFiles, EngineError> {
        let mut rows = 0u64;
        let files = if self.spec.is_unpartitioned() {
            let mut writer = DataFileWriterBuilder::new(self.builder.clone())
                .build(None)
                .await?;
            for batch in batches {
                rows += u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
                if batch.num_rows() > 0 {
                    writer.write(batch).await?;
                }
            }
            writer.close().await?
        } else {
            // A request's rows arrive in any order, so every partition's writer stays open
            // until the end rather than closing when the next partition starts.
            let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
                self.schema.clone(),
                self.spec.clone(),
            )?;
            let mut writer = FanoutWriter::new(DataFileWriterBuilder::new(self.builder.clone()));
            for batch in batches {
                rows += u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
                if batch.num_rows() == 0 {
                    continue;
                }
                for (partition, part) in splitter.split(&batch)? {
                    writer.write(partition, part).await?;
                }
            }
            writer.close().await?
        };
        Ok(WrittenFiles {
            files,
            table_uuid: self.table_uuid,
            rows,
        })
    }
}
