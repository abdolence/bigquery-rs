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
        let mut sink = self.open().await?;
        for batch in batches {
            sink.write(batch).await?;
        }
        sink.close().await
    }

    /// A sink that takes batches one at a time, so that a result is written as it is produced.
    pub(crate) async fn open(&self) -> Result<FileSink, EngineError> {
        let data_files = DataFileWriterBuilder::new(self.builder.clone());
        let writer = if self.spec.is_unpartitioned() {
            SinkWriter::Unpartitioned(Box::new(data_files.build(None).await?))
        } else {
            SinkWriter::Partitioned(Box::new(Fanout {
                splitter: RecordBatchPartitionSplitter::try_new_with_computed_values(
                    self.schema.clone(),
                    self.spec.clone(),
                )?,
                // A request's rows arrive in any order, so every partition's writer stays
                // open until the end rather than closing when the next partition starts.
                writer: FanoutWriter::new(data_files),
            }))
        };
        Ok(FileSink {
            writer,
            table_uuid: self.table_uuid,
            rows: 0,
        })
    }
}

/// Data files of one table identity being written, batch by batch.
pub(crate) struct FileSink {
    writer: SinkWriter,
    table_uuid: Uuid,
    rows: u64,
}

/// The data file writer of a table's files, below any partitioning.
type DataFiles =
    DataFileWriterBuilder<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;

enum SinkWriter {
    Unpartitioned(Box<<DataFiles as IcebergWriterBuilder>::R>),
    Partitioned(Box<Fanout>),
}

/// A writer per partition, each fed the rows the splitter assigns it.
struct Fanout {
    splitter: RecordBatchPartitionSplitter,
    writer: FanoutWriter<DataFiles>,
}

impl FileSink {
    /// Writes `batch`, whose schema is the table's engine Arrow schema with its field ids.
    pub(crate) async fn write(&mut self, batch: RecordBatch) -> Result<(), EngineError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.rows = self
            .rows
            .saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
        match &mut self.writer {
            SinkWriter::Unpartitioned(writer) => writer.write(batch).await?,
            SinkWriter::Partitioned(fanout) => {
                for (partition, part) in fanout.splitter.split(&batch)? {
                    fanout.writer.write(partition, part).await?;
                }
            }
        }
        Ok(())
    }

    /// Closes the files and returns them. Nothing references them until a commit adds them.
    pub(crate) async fn close(self) -> Result<WrittenFiles, EngineError> {
        let files = match self.writer {
            SinkWriter::Unpartitioned(mut writer) => writer.close().await?,
            SinkWriter::Partitioned(fanout) => fanout.writer.close().await?,
        };
        Ok(WrittenFiles {
            files,
            table_uuid: self.table_uuid,
            rows: self.rows,
        })
    }
}
