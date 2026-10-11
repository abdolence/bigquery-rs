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
use crate::ids::RowOffset;
use crate::storage::{OffsetRanges, StagedFile};
use arrow::array::{RecordBatch, UInt32Array};
use arrow::compute::take_record_batch;
use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use futures::TryStreamExt;
use iceberg::arrow::{arrow_struct_to_literal, schema_to_arrow_schema, PartitionValueCalculator};
use iceberg::scan::FileScanTask;
use iceberg::spec::{
    DataFile, DataFileFormat, Literal, PartitionKey, PartitionSpecRef, SchemaRef, Struct,
};
use iceberg::table::Table;
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
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

type FileWriterBuilder = RollingFileWriterBuilder<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

/// The data file writer of a table's files, below any partitioning.
type DataFiles =
    DataFileWriterBuilder<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;

/// The data files a write produced, for the table identity they were written for.
#[derive(Debug, Clone)]
pub(crate) struct WrittenFiles {
    pub files: Vec<DataFile>,
    /// The UUID of the table identity the files were written for: a commit lands them only
    /// while the table under the name is still that identity.
    pub table_uuid: Uuid,
    pub rows: u64,
}

impl WrittenFiles {
    /// The data files of `staged`, written for the table identity `loaded` is, which holds the
    /// schemas they were written with.
    pub(crate) fn staged(staged: &[StagedFile], loaded: &LoadedTable) -> Result<Self, EngineError> {
        Ok(Self {
            files: staged
                .iter()
                .map(|file| file.data_file(loaded.metadata()))
                .collect::<Result<_, _>>()?,
            table_uuid: loaded.metadata().uuid(),
            rows: staged.iter().map(|file| file.rows.rows()).sum(),
        })
    }
}

/// Writes batches into data files of one table identity, as its current schema and partition
/// spec define them.
pub(crate) struct TableWriter {
    table: Table,
    builder: FileWriterBuilder,
    schema: SchemaRef,
    arrow_schema: ArrowSchemaRef,
    spec: PartitionSpecRef,
    /// `None` for an unpartitioned table.
    partitioner: Option<Arc<Partitioner>>,
}

/// The rows of a batch that fall into one partition, by index in the batch, ascending.
struct Part {
    key: PartitionKey,
    rows: Vec<u32>,
}

impl Part {
    /// This part's rows of `batch`, in order.
    fn take(&self, batch: &RecordBatch) -> Result<RecordBatch, EngineError> {
        take_record_batch(batch, &UInt32Array::from(self.rows.clone())).map_err(|err| {
            EngineError::Internal {
                detail: format!("the rows of a partition do not form a batch: {err}"),
            }
        })
    }
}

/// Groups a batch's rows by the partition the table's spec puts them in. Iceberg's own
/// splitter does the same but does not say which rows went where, which a staged file must
/// record.
struct Partitioner {
    calculator: PartitionValueCalculator,
    spec: PartitionSpecRef,
    schema: SchemaRef,
}

impl Partitioner {
    /// The batch's rows by partition, the partitions in the order of their first rows.
    fn split(&self, batch: &RecordBatch) -> Result<Vec<Part>, EngineError> {
        let values = arrow_struct_to_literal(
            &self.calculator.calculate(batch)?,
            self.calculator.partition_type(),
        )?;
        let mut parts: Vec<Part> = Vec::new();
        let mut by_value: HashMap<Struct, usize> = HashMap::new();
        for (row, value) in (0u32..).zip(values) {
            let Some(Literal::Struct(value)) = value else {
                return Err(EngineError::Internal {
                    detail: format!("row {row} of a batch has no partition value"),
                });
            };
            let part = *by_value.entry(value.clone()).or_insert_with(|| {
                parts.push(Part {
                    key: PartitionKey::new(self.spec.as_ref().clone(), self.schema.clone(), value),
                    rows: Vec::new(),
                });
                parts.len() - 1
            });
            parts[part].rows.push(row);
        }
        Ok(parts)
    }
}

impl TableWriter {
    /// A writer of new files of `table`, named after a fresh writer UUID.
    pub(crate) fn new(table: &LoadedTable) -> Result<Self, EngineError> {
        let metadata = table.metadata();
        let schema = metadata.current_schema().clone();
        let spec = metadata.default_partition_spec().clone();
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
        let partitioner = if spec.is_unpartitioned() {
            None
        } else {
            Some(Arc::new(Partitioner {
                calculator: PartitionValueCalculator::try_new(&spec, &schema)?,
                spec: spec.clone(),
                schema: schema.clone(),
            }))
        };
        Ok(Self {
            table: table.table.clone(),
            builder: rolling,
            arrow_schema: Arc::new(schema_to_arrow_schema(&schema)?),
            schema,
            spec,
            partitioner,
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
        let writer = match &self.partitioner {
            None => SinkWriter::Unpartitioned(Box::new(data_files.build(None).await?)),
            Some(partitioner) => SinkWriter::Partitioned(Box::new(Fanout {
                partitioner: Arc::clone(partitioner),
                // A request's rows arrive in any order, so every partition's writer stays
                // open until the end rather than closing when the next partition starts.
                writer: FanoutWriter::new(data_files),
            })),
        };
        Ok(FileSink {
            writer,
            table_uuid: self.table.metadata().uuid(),
            rows: 0,
        })
    }

    /// Writes one request's rows, whose offsets start at `first`, as staged files. A file
    /// holds the rows of one partition in the request's order, so on a partitioned table its
    /// offsets need not be consecutive.
    pub(crate) async fn write_staged(
        &self,
        batch: RecordBatch,
        first: RowOffset,
    ) -> Result<Vec<StagedFile>, EngineError> {
        let offset = |row: u32| first.after(u64::from(row));
        let parts = match &self.partitioner {
            None => {
                let rows = u32::try_from(batch.num_rows()).map_err(|_| EngineError::Internal {
                    detail: format!("a request of {} rows", batch.num_rows()),
                })?;
                vec![(
                    None,
                    OffsetRanges::from_ascending((0..rows).map(offset)),
                    batch,
                )]
            }
            Some(partitioner) => partitioner
                .split(&batch)?
                .into_iter()
                .map(|part| {
                    let rows = part.take(&batch)?;
                    let offsets = OffsetRanges::from_ascending(part.rows.into_iter().map(offset));
                    Ok((Some(part.key), offsets, rows))
                })
                .collect::<Result<_, EngineError>>()?,
        };
        let mut staged = Vec::new();
        for (key, offsets, rows) in parts {
            let files = self.write_partition(key, [rows]).await?;
            staged.extend(self.staged(files, offsets)?);
        }
        Ok(staged)
    }

    /// Rewrites the staged file `file` as staged files of the same partition: its rows below
    /// `end`, and the rest, each in the file's row order. The original stays as it is.
    pub(crate) async fn split(
        &self,
        file: &StagedFile,
        end: RowOffset,
    ) -> Result<(Vec<StagedFile>, Vec<StagedFile>), EngineError> {
        let data_file = file.data_file(self.table.metadata())?;
        let key = self.partitioner.as_ref().map(|_| {
            PartitionKey::new(
                self.spec.as_ref().clone(),
                self.schema.clone(),
                data_file.partition().clone(),
            )
        });
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(data_file.file_size_in_bytes())
            .with_start(0)
            .with_length(data_file.file_size_in_bytes())
            .with_record_count(Some(data_file.record_count()))
            .with_data_file_path(data_file.file_path().to_string())
            .with_data_file_format(data_file.file_format())
            .with_schema(self.schema.clone())
            .with_project_field_ids(
                self.schema
                    .as_struct()
                    .fields()
                    .iter()
                    .map(|field| field.id)
                    .collect(),
            )
            .with_case_sensitive(true)
            .build();
        let batches: Vec<RecordBatch> = self
            .table
            .reader_builder()
            .with_data_file_concurrency_limit(1)
            .build()
            .read(Box::pin(futures::stream::iter([Ok(task)])))?
            .stream()
            .try_collect()
            .await?;
        let keep = file.rows.rows_before(end);
        let (head_offsets, tail_offsets) = file.rows.clone().split(keep);
        let mut head = Vec::new();
        let mut tail = Vec::new();
        let mut left = keep;
        for batch in batches {
            // The reader's batches carry the table's columns in the table's types; the writer
            // also wants the field ids on the schema.
            let batch = RecordBatch::try_new(self.arrow_schema.clone(), batch.columns().to_vec())
                .map_err(|err| EngineError::Internal {
                detail: format!(
                    "{} reads back in another schema than the table's: {err}",
                    data_file.file_path()
                ),
            })?;
            let rows = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
            let cut = left.min(rows);
            left -= cut;
            let cut = usize::try_from(cut).unwrap_or(batch.num_rows());
            head.push(batch.slice(0, cut));
            tail.push(batch.slice(cut, batch.num_rows() - cut));
        }
        let head = self.write_partition(key.clone(), head).await?;
        let tail = self.write_partition(key, tail).await?;
        Ok((
            self.staged(head, head_offsets)?,
            self.staged(tail, tail_offsets)?,
        ))
    }

    /// Writes `batches` as files of the partition `key`, in order; none for no rows.
    async fn write_partition(
        &self,
        key: Option<PartitionKey>,
        batches: impl IntoIterator<Item = RecordBatch>,
    ) -> Result<Vec<DataFile>, EngineError> {
        let mut batches = batches
            .into_iter()
            .filter(|batch| batch.num_rows() > 0)
            .peekable();
        if batches.peek().is_none() {
            return Ok(Vec::new());
        }
        let mut writer = DataFileWriterBuilder::new(self.builder.clone())
            .build(key)
            .await?;
        for batch in batches {
            writer.write(batch).await?;
        }
        Ok(writer.close().await?)
    }

    /// `files`, written in order from the rows whose offsets are `offsets`, as staged files
    /// that each hold the offsets of their own rows.
    fn staged(
        &self,
        files: Vec<DataFile>,
        mut offsets: OffsetRanges,
    ) -> Result<Vec<StagedFile>, EngineError> {
        files
            .into_iter()
            .map(|file| {
                let (own, rest) = std::mem::take(&mut offsets).split(file.record_count());
                offsets = rest;
                StagedFile::new(file, own, self.table.metadata())
            })
            .collect()
    }
}

/// Data files of one table identity being written, batch by batch.
pub(crate) struct FileSink {
    writer: SinkWriter,
    table_uuid: Uuid,
    rows: u64,
}

enum SinkWriter {
    Unpartitioned(Box<<DataFiles as IcebergWriterBuilder>::R>),
    Partitioned(Box<Fanout>),
}

/// A writer per partition, each fed the rows the partitioner assigns it.
struct Fanout {
    partitioner: Arc<Partitioner>,
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
                for part in fanout.partitioner.split(&batch)? {
                    let rows = part.take(&batch)?;
                    fanout.writer.write(part.key, rows).await?;
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
