//! The write path: an `AppendRows` request becomes one Arrow batch in the table's engine types
//! ([`rows`]), the batch becomes data files of the table written without any lock ([`files`]),
//! and the files join the table's commit group, which lands them in one `fast_append` with the
//! other requests that arrived meanwhile ([`commit`]).
//!
//! The `_default` stream's rows are visible once acknowledged, at least once, as in BigQuery.
//! The streams `CreateWriteStream` makes take each request at an offset, so a request sent
//! again lands once ([`modes`]).

mod commit;
mod files;
mod modes;
mod rows;

pub(crate) use commit::CommitGroups;
pub(crate) use files::{FileSink, TableWriter, WrittenFiles};
pub(crate) use rows::{ArrowRows, MissingValues, Rejected, RequestRows, WriteTarget, WriterSchema};

use crate::catalog::{EngineCatalog, LoadedTable};
use crate::error::{EngineError, Resource, StreamProblem};
use crate::ids::{RowOffset, StreamId, TableRef, WriteStreamName};
use crate::types::{ColumnName, Columns};
use gcloud_sdk::google::cloud::bigquery::storage::v1 as storage;
use gcloud_sdk::google::rpc;
use gcloud_sdk::prost::Message;
use modes::Streams;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};
use storage::append_rows_request::{MissingValueInterpretation, Rows};
use storage::append_rows_response::{AppendResult, Response};
use storage::storage_error::StorageErrorCode;
use storage::write_stream::Type;
use tonic::Code;
use tracing::debug;

/// The write side of one engine: the catalog, every table's commit group and the created
/// write streams.
#[derive(Debug)]
pub(crate) struct Writes {
    catalog: Arc<EngineCatalog>,
    groups: Arc<CommitGroups>,
    streams: Arc<Streams>,
    /// Set by a test so that the next `AppendRows` response is never sent and its connection
    /// ends, as when the network drops it after the request was handled.
    #[cfg(test)]
    lose_response: std::sync::atomic::AtomicBool,
}

/// What an append requires of the table it lands in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteDisposition {
    /// `WRITE_APPEND`: any table.
    Append,
    /// `WRITE_EMPTY`: a table that holds no rows.
    Empty,
}

impl Writes {
    pub(crate) fn new(
        catalog: Arc<EngineCatalog>,
        commit_window: Duration,
        stream_ttl: Duration,
    ) -> Self {
        let groups = Arc::new(CommitGroups::new(catalog.clone(), commit_window));
        Self {
            streams: Arc::new(Streams::new(catalog.clone(), groups.clone(), stream_ttl)),
            groups,
            catalog,
            #[cfg(test)]
            lose_response: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// The write stream `name` names, as `GetWriteStream` reports it.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] for a missing table or stream.
    pub(crate) async fn stream(&self, name: &str) -> Result<storage::WriteStream, EngineError> {
        self.describe(&name.parse()?).await
    }

    async fn describe(&self, name: &WriteStreamName) -> Result<storage::WriteStream, EngineError> {
        let loaded = self.catalog.load(&name.table).await?;
        let dataset = self.catalog.dataset(&name.table.dataset_ref()).await?;
        let (kind, create_time, commit_time) = match name.stream {
            StreamId::Default => (
                Type::Committed,
                loaded.creation_time_ms()?.and_then(proto_time),
                None,
            ),
            StreamId::Created(_) => {
                let stream = self.streams.describe(name, &loaded).await?;
                (
                    stream.kind,
                    proto_time(stream.created_at_ms),
                    stream.committed_at_ms.and_then(proto_time),
                )
            }
        };
        let metadata = loaded.metadata();
        let columns = Columns::from_iceberg(metadata.current_schema(), metadata.properties())?;
        Ok(storage::WriteStream {
            name: name.to_string(),
            r#type: kind.into(),
            create_time,
            commit_time,
            table_schema: Some(storage::TableSchema::from(&columns)),
            write_mode: storage::write_stream::WriteMode::Insert.into(),
            location: dataset.location.to_string(),
        })
    }

    /// Creates the stream `CreateWriteStream` asks for, and reports it as `GetWriteStream`
    /// does.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] for a missing table, [`EngineError::Invalid`] for a type that
    /// is not committed, pending or buffered.
    pub(crate) async fn create_stream(
        &self,
        request: &storage::CreateWriteStreamRequest,
    ) -> Result<storage::WriteStream, EngineError> {
        let table = TableRef::from_storage_path(&request.parent)?;
        let kind = request
            .write_stream
            .as_ref()
            .map_or(Type::Unspecified, storage::WriteStream::r#type);
        let name = self.streams.create(&table, kind).await?;
        self.streams.sweep_soon();
        self.describe(&name).await
    }

    /// Finalizes the stream `name` names and reports its row count.
    ///
    /// # Errors
    /// As [`Streams::finalize`], and [`EngineError::WriteStream`] for the default stream.
    pub(crate) async fn finalize(
        &self,
        name: &str,
    ) -> Result<storage::FinalizeWriteStreamResponse, EngineError> {
        let rows = self.streams.finalize(&name.parse()?).await?;
        Ok(storage::FinalizeWriteStreamResponse {
            row_count: rows.into(),
        })
    }

    /// Flushes a buffered stream to the offset the request names.
    ///
    /// # Errors
    /// As [`Streams::flush`], and [`EngineError::Invalid`] for a request without an offset.
    pub(crate) async fn flush(
        &self,
        request: &storage::FlushRowsRequest,
    ) -> Result<storage::FlushRowsResponse, EngineError> {
        let offset = request.offset.ok_or_else(|| {
            EngineError::invalid("offset", "FlushRows names the offset to flush to")
        })?;
        let flushed = self
            .streams
            .flush(&request.write_stream.parse()?, RowOffset::try_from(offset)?)
            .await?;
        Ok(storage::FlushRowsResponse {
            offset: flushed.into(),
        })
    }

    /// Commits the pending streams the request names in one commit.
    ///
    /// # Errors
    /// As [`Streams::batch_commit`].
    pub(crate) async fn batch_commit(
        &self,
        request: &storage::BatchCommitWriteStreamsRequest,
    ) -> Result<storage::BatchCommitWriteStreamsResponse, EngineError> {
        let table = TableRef::from_storage_path(&request.parent)?;
        self.streams
            .batch_commit(&table, &request.write_streams)
            .await
    }

    /// Starts dropping the write streams on the root past their TTL.
    pub(crate) fn start_stream_sweep(&self) {
        self.streams.sweep_soon();
    }

    /// Drops the write streams on the root past their TTL, after the sweep running, if any.
    ///
    /// # Errors
    /// As [`Streams::sweep`].
    #[cfg(test)]
    pub(crate) async fn sweep_streams(&self) -> Result<(), EngineError> {
        self.streams.sweep().await
    }

    /// The state of a new `AppendRows` connection.
    pub(crate) fn connection(self: &Arc<Self>) -> AppendConnection {
        AppendConnection {
            writes: Arc::clone(self),
            stream: None,
            writer: None,
            schema_id: None,
        }
    }

    /// Writes `batch`'s rows as data files of `loaded` and lands them with the table's commit
    /// group.
    async fn append(
        &self,
        table: &TableRef,
        loaded: &LoadedTable,
        batch: arrow::array::RecordBatch,
    ) -> Result<(), EngineError> {
        let files = TableWriter::new(loaded)?.write([batch]).await?;
        self.append_files(table, files, WriteDisposition::Append)
            .await
    }

    /// Lands data files written for `table` with the table's commit group, under
    /// `disposition`.
    pub(crate) async fn append_files(
        &self,
        table: &TableRef,
        files: WrittenFiles,
        disposition: WriteDisposition,
    ) -> Result<(), EngineError> {
        self.groups.append(table, files, disposition).await
    }

    #[cfg(test)]
    pub(crate) fn groups(&self) -> &Arc<CommitGroups> {
        &self.groups
    }

    #[cfg(test)]
    pub(crate) fn lose_next_response(&self) {
        self.lose_response
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Whether the next response is still to be lost.
    #[cfg(test)]
    pub(crate) fn loses_a_response(&self) -> bool {
        self.lose_response.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Whether the response at hand is the one to lose; it is lost once.
    #[cfg(test)]
    pub(crate) fn take_lost_response(&self) -> bool {
        self.lose_response
            .swap(false, std::sync::atomic::Ordering::SeqCst)
    }
}

/// One `AppendRows` connection: the stream and the writer schema its first request named, which
/// later requests may leave out, and the table schema it last reported.
pub(crate) struct AppendConnection {
    writes: Arc<Writes>,
    stream: Option<WriteStreamName>,
    writer: Option<WriterSchema>,
    schema_id: Option<i32>,
}

impl AppendConnection {
    /// Handles one request: all of its rows are written, or none, and the response says which.
    pub(crate) async fn append(
        &mut self,
        request: storage::AppendRowsRequest,
    ) -> storage::AppendRowsResponse {
        let named = request.write_stream.clone();
        let outcome = self.try_append(request).await;
        let write_stream = self.stream.as_ref().map_or(named, ToString::to_string);
        let mut response = storage::AppendRowsResponse {
            write_stream,
            ..Default::default()
        };
        match outcome {
            Ok(appended) => {
                response.response = Some(Response::AppendResult(AppendResult {
                    offset: appended.offset.map(i64::from),
                }));
                response.updated_schema = appended.updated_schema;
            }
            Err(Rejected::Rows(errors)) => {
                response.response = Some(Response::Error(rpc::Status {
                    code: Code::InvalidArgument as i32,
                    message: format!(
                        "{} row(s) of the request do not fit the table; nothing was written",
                        errors.len()
                    ),
                    details: Vec::new(),
                }));
                response.row_errors = errors
                    .into_iter()
                    .map(|error| storage::RowError {
                        index: i64::try_from(error.index).unwrap_or(i64::MAX),
                        code: storage::row_error::RowErrorCode::FieldsError.into(),
                        message: error.message,
                    })
                    .collect();
            }
            Err(Rejected::ExtraFields(fields)) => {
                let message = format!(
                    "Input schema has more fields than BigQuery schema, extra fields: {}",
                    fields.join(", ")
                );
                response.response = Some(Response::Error(in_band(
                    Code::InvalidArgument,
                    Some(StorageErrorCode::SchemaMismatchExtraFields),
                    &response.write_stream,
                    message,
                )));
            }
            Err(Rejected::Request(error)) => {
                let at_least_once = self
                    .stream
                    .as_ref()
                    .is_none_or(|stream| stream.stream == StreamId::Default);
                response.response = Some(Response::Error(
                    error.in_band(&response.write_stream, at_least_once),
                ));
            }
        }
        response
    }

    /// The request's rows written and landed, or staged on a pending or buffered stream.
    async fn try_append(
        &mut self,
        request: storage::AppendRowsRequest,
    ) -> Result<Appended, Rejected> {
        if !request.write_stream.is_empty() {
            let named: WriteStreamName = request.write_stream.parse()?;
            if self.stream.as_ref().is_some_and(|stream| *stream != named) {
                return Err(EngineError::invalid(
                    "write_stream",
                    "a connection writes to the stream its first request named",
                )
                .into());
            }
            self.stream = Some(named);
        }
        let stream = self.stream.clone().ok_or_else(|| {
            EngineError::invalid("write_stream", "the first request names the stream")
        })?;
        let offset = request.offset.map(RowOffset::try_from).transpose()?;
        match (stream.stream, offset) {
            (StreamId::Default, Some(_)) => {
                return Err(
                    EngineError::invalid("offset", "the _default stream takes no offsets").into(),
                )
            }
            (StreamId::Created(_), None) => {
                return Err(EngineError::invalid(
                    "offset",
                    "a created stream takes each request at an offset",
                )
                .into())
            }
            _ => {}
        }
        let rows = match &request.rows {
            Some(Rows::ProtoRows(proto)) => {
                if let Some(schema) = &proto.writer_schema {
                    self.writer = Some(WriterSchema::try_from(schema)?);
                }
                RequestRows::Proto(
                    proto
                        .rows
                        .as_ref()
                        .map(|rows| rows.serialized_rows.as_slice())
                        .unwrap_or_default(),
                )
            }
            Some(Rows::ArrowRows(arrow)) => {
                if let Some(schema) = &arrow.writer_schema {
                    self.writer = Some(WriterSchema::Arrow(ArrowRows::new(
                        schema.serialized_schema.clone(),
                    )));
                }
                RequestRows::Arrow(
                    arrow
                        .rows
                        .as_ref()
                        .map(|rows| rows.serialized_record_batch.as_slice())
                        .unwrap_or_default(),
                )
            }
            None => {
                return Ok(Appended {
                    offset: None,
                    updated_schema: None,
                })
            }
        };
        let writer = self.writer.as_ref().ok_or_else(|| {
            EngineError::invalid(
                "writer_schema",
                "the first request carries the writer schema",
            )
        })?;
        if stream.stream != StreamId::Default && writer.declares_change_type() {
            return Err(EngineError::invalid(
                "_CHANGE_TYPE",
                "change batches go to the _default stream only",
            )
            .into());
        }
        let loaded = self.writes.catalog.load(&stream.table).await?;
        let target = WriteTarget::new(&loaded)?;
        let schema_id = loaded.metadata().current_schema_id();
        let updated_schema = match self.schema_id.replace(schema_id) {
            Some(previous) if previous != schema_id => {
                Some(storage::TableSchema::from(target.columns()))
            }
            _ => None,
        };
        let batch = target.batch(writer, rows, &MissingValues::try_from(&request)?)?;
        let rows = batch.num_rows();
        let offset = match offset {
            Some(offset) => Some(
                self.writes
                    .streams
                    .append(&stream, offset, &loaded, batch)
                    .await?,
            ),
            None if rows > 0 => {
                self.writes.append(&stream.table, &loaded, batch).await?;
                None
            }
            None => None,
        };
        debug!(%stream, rows, "Appended a request's rows.");
        Ok(Appended {
            offset,
            updated_schema,
        })
    }
}

/// A request handled.
struct Appended {
    /// The offset of its rows, on a stream with offsets.
    offset: Option<RowOffset>,
    /// The table schema to report, when it changed since the connection last reported one.
    updated_schema: Option<storage::TableSchema>,
}

/// A request's missing value interpretations.
///
/// # Errors
/// [`EngineError::Invalid`] for an interpretation this API version does not define and for a
/// key that is no column name: neither can be applied as the writer meant it.
impl TryFrom<&storage::AppendRowsRequest> for MissingValues {
    type Error = EngineError;

    fn try_from(request: &storage::AppendRowsRequest) -> Result<Self, Self::Error> {
        let interpretation = |field: String, value: i32| {
            MissingValueInterpretation::try_from(value).map_err(|_| {
                EngineError::invalid(
                    field,
                    format!("{value} is not a missing value interpretation"),
                )
            })
        };
        Ok(MissingValues {
            default: interpretation(
                "default_missing_value_interpretation".to_string(),
                request.default_missing_value_interpretation,
            )?,
            per_column: request
                .missing_value_interpretations
                .iter()
                .map(|(column, value)| {
                    Ok((
                        ColumnName::try_from(column.clone())?,
                        interpretation(
                            format!("missing_value_interpretations[{column:?}]"),
                            *value,
                        )?,
                    ))
                })
                .collect::<Result<_, EngineError>>()?,
        })
    }
}

impl EngineError {
    /// The `StorageError` code the Storage API reports this error with, which the client
    /// branches on.
    fn storage_code(&self) -> Option<StorageErrorCode> {
        match self {
            EngineError::NotFound {
                what: Resource::Table(_),
            } => Some(StorageErrorCode::TableNotFound),
            EngineError::NotFound {
                what: Resource::WriteStream(_),
            } => Some(StorageErrorCode::StreamNotFound),
            EngineError::WriteStream { problem, .. } => Some(problem.storage_code()),
            _ => None,
        }
    }

    /// This error of a write stream as the `StorageError` a batch commit reports for it.
    pub(crate) fn stream_error(&self) -> Option<storage::StorageError> {
        let (EngineError::NotFound {
            what: Resource::WriteStream(name),
        }
        | EngineError::WriteStream {
            what: Resource::WriteStream(name),
            ..
        }) = self
        else {
            return None;
        };
        Some(storage_error(
            self.storage_code()?,
            &name.to_string(),
            self.to_string(),
        ))
    }

    /// This error as an in-band `AppendRows` status about `stream`, with the `StorageError`
    /// the client branches on. A commit whose outcome is unknown answers `Internal` on an
    /// `at_least_once` stream, which the client sends again, and `Unknown` on a stream with
    /// offsets, which it does not.
    fn in_band(&self, stream: &str, at_least_once: bool) -> rpc::Status {
        let code = match self {
            EngineError::CommitOutcomeUnknown { .. } if at_least_once => Code::Internal,
            other => tonic::Status::from(other).code(),
        };
        in_band(code, self.storage_code(), stream, self.to_string())
    }
}

impl StreamProblem {
    fn storage_code(self) -> StorageErrorCode {
        match self {
            StreamProblem::OffsetAlreadyExists { .. } => StorageErrorCode::OffsetAlreadyExists,
            StreamProblem::OffsetOutOfRange { .. } => StorageErrorCode::OffsetOutOfRange,
            StreamProblem::Finalized => StorageErrorCode::StreamFinalized,
            StreamProblem::AlreadyCommitted => StorageErrorCode::StreamAlreadyCommitted,
            StreamProblem::NotFinalized
            | StreamProblem::OutcomeUnknown
            | StreamProblem::ChangedElsewhere => StorageErrorCode::InvalidStreamState,
            StreamProblem::WrongType { .. } => StorageErrorCode::InvalidStreamType,
            StreamProblem::Expired | StreamProblem::CreatedElsewhere => {
                StorageErrorCode::StreamNotFound
            }
        }
    }
}

/// A `StorageError` of `code` about `entity`.
fn storage_error(code: StorageErrorCode, entity: &str, message: String) -> storage::StorageError {
    storage::StorageError {
        code: code.into(),
        entity: entity.to_string(),
        error_message: message,
    }
}

/// An in-band status of `code`, carrying a `StorageError` of `storage` about `entity` when set.
fn in_band(
    code: Code,
    storage: Option<StorageErrorCode>,
    entity: &str,
    message: String,
) -> rpc::Status {
    let details = storage
        .map(|storage| gcloud_sdk::prost_types::Any {
            type_url: "type.googleapis.com/google.cloud.bigquery.storage.v1.StorageError"
                .to_string(),
            value: storage_error(storage, entity, message.clone()).encode_to_vec(),
        })
        .into_iter()
        .collect();
    rpc::Status {
        code: code as i32,
        message,
        details,
    }
}

/// `ms` milliseconds after the Unix epoch, as the APIs carry a time; `None` before the epoch.
pub(crate) fn proto_time(ms: i64) -> Option<gcloud_sdk::prost_types::Timestamp> {
    u64::try_from(ms)
        .ok()
        .map(|ms| (UNIX_EPOCH + Duration::from_millis(ms)).into())
}

#[cfg(test)]
mod tests;
