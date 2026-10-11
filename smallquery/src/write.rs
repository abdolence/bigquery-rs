//! The write path: an `AppendRows` request becomes one Arrow batch in the table's engine types
//! ([`rows`]), the batch becomes data files of the table written without any lock ([`files`]),
//! and the files join the table's commit group, which lands them in one `fast_append` with the
//! other requests that arrived meanwhile ([`commit`]).
//!
//! This release serves the `_default` stream: each request's rows are visible once
//! acknowledged, at least once, as in BigQuery.

mod commit;
mod files;
mod rows;

pub(crate) use commit::{Appended, CommitGroups};
pub(crate) use files::{TableWriter, WrittenFiles};
pub(crate) use rows::{ArrowRows, MissingValues, Rejected, RequestRows, WriteTarget, WriterSchema};

use crate::catalog::{EngineCatalog, LoadedTable};
use crate::error::{EngineError, Resource};
use crate::ids::TableRef;
use crate::services::timestamp;
use crate::types::Columns;
use gcloud_sdk::google::cloud::bigquery::storage::v1 as storage;
use gcloud_sdk::google::rpc;
use gcloud_sdk::prost::Message;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};
use storage::append_rows_request::{MissingValueInterpretation, Rows};
use storage::append_rows_response::{AppendResult, Response};
use storage::storage_error::StorageErrorCode;
use tonic::Code;
use tracing::debug;

/// The write side of one engine: the catalog and every table's commit group.
#[derive(Debug)]
pub(crate) struct Writes {
    catalog: Arc<EngineCatalog>,
    groups: Arc<CommitGroups>,
}

/// A write stream's name: `projects/{project}/datasets/{dataset}/tables/{table}/streams/{id}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriteStreamName {
    table: TableRef,
    stream: StreamId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StreamId {
    /// `_default`, which every table has without creating it.
    Default,
    /// A stream `CreateWriteStream` created.
    Created(String),
}

impl std::str::FromStr for WriteStreamName {
    type Err = EngineError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        let (table, stream) = name.rsplit_once("/streams/").ok_or_else(|| {
            EngineError::invalid(
                format!("write stream {name:?}"),
                "a write stream is named {table}/streams/{id}",
            )
        })?;
        Ok(Self {
            table: TableRef::from_storage_path(table)?,
            stream: match stream {
                "_default" => StreamId::Default,
                id => StreamId::Created(id.to_string()),
            },
        })
    }
}

impl std::fmt::Display for WriteStreamName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stream = match &self.stream {
            StreamId::Default => "_default",
            StreamId::Created(id) => id,
        };
        write!(f, "{}/streams/{stream}", self.table.storage_path())
    }
}

impl Writes {
    pub(crate) fn new(catalog: Arc<EngineCatalog>, commit_window: Duration) -> Self {
        Self {
            groups: Arc::new(CommitGroups::new(catalog.clone(), commit_window)),
            catalog,
        }
    }

    /// The write stream `name` names, as `GetWriteStream` reports it.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] for a missing table and for any stream but `_default`.
    pub(crate) async fn stream(&self, name: &str) -> Result<storage::WriteStream, EngineError> {
        let parsed: WriteStreamName = name.parse()?;
        if parsed.stream != StreamId::Default {
            return Err(EngineError::NotFound {
                what: Resource::WriteStream(name.to_string()),
            });
        }
        let loaded = self.catalog.load(&parsed.table).await?;
        let dataset = self.catalog.dataset(&parsed.table.dataset_ref()).await?;
        let metadata = loaded.metadata();
        let columns = Columns::from_iceberg(metadata.current_schema(), metadata.properties())?;
        let created_ms = metadata
            .properties()
            .get("bigquery.creation_time_ms")
            .and_then(|ms| ms.parse::<u64>().ok())
            .unwrap_or_default();
        Ok(storage::WriteStream {
            name: parsed.to_string(),
            r#type: storage::write_stream::Type::Committed.into(),
            create_time: Some(timestamp(UNIX_EPOCH + Duration::from_millis(created_ms))),
            table_schema: Some(storage::TableSchema::from(&columns)),
            write_mode: storage::write_stream::WriteMode::Insert.into(),
            location: dataset.location.to_string(),
            ..Default::default()
        })
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
    ) -> Result<Appended, EngineError> {
        let files = TableWriter::new(loaded)?.write([batch]).await?;
        self.append_files(table, files).await
    }

    /// Lands data files written for `table` with the table's commit group.
    pub(crate) async fn append_files(
        &self,
        table: &TableRef,
        files: WrittenFiles,
    ) -> Result<Appended, EngineError> {
        self.groups.append(table, files).await
    }

    #[cfg(test)]
    pub(crate) fn groups(&self) -> &Arc<CommitGroups> {
        &self.groups
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
            Ok(updated_schema) => {
                response.response = Some(Response::AppendResult(AppendResult { offset: None }));
                response.updated_schema = updated_schema;
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
                response.response = Some(Response::Error(error.in_band(&response.write_stream)));
            }
        }
        response
    }

    /// The request's rows written and landed; returns the table schema to report when it
    /// changed since the connection last reported one.
    async fn try_append(
        &mut self,
        request: storage::AppendRowsRequest,
    ) -> Result<Option<storage::TableSchema>, Rejected> {
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
        if let StreamId::Created(_) = stream.stream {
            return Err(EngineError::NotFound {
                what: Resource::WriteStream(stream.to_string()),
            }
            .into());
        }
        if request.offset.is_some() {
            return Err(
                EngineError::invalid("offset", "the _default stream takes no offsets").into(),
            );
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
            None => return Ok(None),
        };
        let writer = self.writer.as_ref().ok_or_else(|| {
            EngineError::invalid(
                "writer_schema",
                "the first request carries the writer schema",
            )
        })?;
        let loaded = self.writes.catalog.load(&stream.table).await?;
        let target = WriteTarget::new(&loaded)?;
        let schema_id = loaded.metadata().current_schema_id();
        let updated_schema = match self.schema_id.replace(schema_id) {
            Some(previous) if previous != schema_id => {
                Some(storage::TableSchema::from(target.columns()))
            }
            _ => None,
        };
        let batch = target.batch(writer, rows, &MissingValues::from(&request))?;
        let rows = batch.num_rows();
        if rows > 0 {
            let appended = self.writes.append(&stream.table, &loaded, batch).await?;
            debug!(
                table = %stream.table,
                rows,
                snapshot_id = appended.snapshot_id,
                pointer_version = appended.pointer_version.number(),
                "Appended a request's rows.",
            );
        }
        Ok(updated_schema)
    }
}

impl From<&storage::AppendRowsRequest> for MissingValues {
    fn from(request: &storage::AppendRowsRequest) -> Self {
        MissingValues {
            default: MissingValueInterpretation::try_from(
                request.default_missing_value_interpretation,
            )
            .unwrap_or_default(),
            per_column: request
                .missing_value_interpretations
                .iter()
                .map(|(column, interpretation)| {
                    (
                        column.clone(),
                        MissingValueInterpretation::try_from(*interpretation).unwrap_or_default(),
                    )
                })
                .collect(),
        }
    }
}

impl EngineError {
    /// This error as an in-band `AppendRows` status, with the `StorageError` the client branches
    /// on: a missing table is `TABLE_NOT_FOUND`, an unknown stream `STREAM_NOT_FOUND`. A commit
    /// whose outcome is unknown answers `Internal`, which the client sends again: the default
    /// stream is at least once.
    fn in_band(&self, stream: &str) -> rpc::Status {
        let (code, storage) = match self {
            EngineError::NotFound {
                what: Resource::Table(_),
            } => (Code::NotFound, Some(StorageErrorCode::TableNotFound)),
            EngineError::NotFound {
                what: Resource::WriteStream(_),
            } => (Code::NotFound, Some(StorageErrorCode::StreamNotFound)),
            EngineError::CommitOutcomeUnknown { .. } => (Code::Internal, None),
            other => (tonic::Status::from(other).code(), None),
        };
        in_band(code, storage, stream, self.to_string())
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
            value: storage::StorageError {
                code: storage.into(),
                entity: entity.to_string(),
                error_message: message.clone(),
            }
            .encode_to_vec(),
        })
        .into_iter()
        .collect();
    rpc::Status {
        code: code as i32,
        message,
        details,
    }
}

#[cfg(test)]
mod tests;
