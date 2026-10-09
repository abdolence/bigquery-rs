//! The Storage Write RPCs: `GetWriteStream`, `CreateWriteStream`, `AppendRows`, `FlushRows`,
//! `FinalizeWriteStream` and `BatchCommitWriteStreams`.
//!
//! Rows become visible as BigQuery makes them visible: on the default and committed streams
//! when acknowledged, on a buffered stream up to the last flushed offset, and on a pending
//! stream at its commit. CDC rows are recorded as changes and never applied.

use crate::db::fake::wire::storage_error_status;
use crate::db::fake::FakeCall;
use crate::read::ArrowIpcDecoder;
use crate::testing::rows::{DecodedRows, ProtoBatchBuilder};
use crate::testing::rules::{BigQueryFakeFault, BigQueryFakeRpc};
use crate::testing::server::FakeShared;
use crate::testing::state::{FakeChanges, FakeState, FakeWriteStream, TableKey};
use crate::{BigQueryInstant, BigQueryTableSchema, BigQueryWriteMode, BigQueryWriteStreamName};
use arrow_array::{new_null_array, RecordBatch, RecordBatchOptions};
use arrow_schema::SchemaRef;
use gcloud_sdk::google::cloud::bigquery::storage::v1 as storage;
use gcloud_sdk::google::rpc;
use gcloud_sdk::prost::Message;
use gcloud_sdk::prost_types::{DescriptorProto, Timestamp};
use gcloud_sdk::tonic::{Code, Status};
use std::panic::{catch_unwind, AssertUnwindSafe};
use storage::append_rows_request::Rows;
use storage::append_rows_response::{AppendResult, Response};
use storage::row_error::RowErrorCode;
use storage::storage_error::StorageErrorCode;
use storage::write_stream::{Type as WriteStreamType, WriteMode};
use storage::{
    AppendRowsRequest, AppendRowsResponse, BatchCommitWriteStreamsRequest,
    BatchCommitWriteStreamsResponse, CreateWriteStreamRequest, FinalizeWriteStreamRequest,
    FinalizeWriteStreamResponse, FlushRowsRequest, FlushRowsResponse, GetWriteStreamRequest,
    RowError, StorageError, WriteStream,
};

/// How a table's default stream name ends, after the table's path.
const DEFAULT_STREAM: &str = "/streams/_default";

impl FakeShared {
    pub(super) async fn serve_write(&self, call: FakeCall) {
        match call.method() {
            "GetWriteStream" => self.get_write_stream(call).await,
            "CreateWriteStream" => self.create_write_stream(call).await,
            "AppendRows" => self.append_rows(call).await,
            "FlushRows" => self.flush_rows(call).await,
            "FinalizeWriteStream" => self.finalize_write_stream(call).await,
            "BatchCommitWriteStreams" => self.batch_commit_write_streams(call).await,
            method => {
                let described = method.to_string();
                self.unmatched(call, &described, &[]);
            }
        }
    }

    /// The table of the Storage API resource `name`. A name of another shape is answered as
    /// unmatched, and `None` is returned.
    fn write_table(&self, call: FakeCall, name: &str) -> Option<(FakeCall, TableKey)> {
        match TableKey::from_path(name) {
            Ok(table) => Some((call, table)),
            Err(_) => {
                let described = format!("{} of {name:?}, which names no table", call.method());
                self.unmatched(call, &described, &[]);
                None
            }
        }
    }

    /// Answers a unary write call on `table` with the first fault for `rpc` on it, or else
    /// with what `answer` makes of the state.
    async fn answer_write<M: Message>(
        &self,
        call: FakeCall,
        rpc: BigQueryFakeRpc,
        table: &TableKey,
        answer: impl FnOnce(&mut FakeState) -> Result<M, Status>,
    ) {
        let fault = self.rules().fault(rpc, Some(table));
        if let Some(fault) = fault {
            fault.answer(call).await;
            return;
        }
        let answered = answer(&mut self.state());
        match answered {
            Ok(message) => call.reply(&message),
            Err(status) => call.fail(status.code(), status.message()),
        }
    }

    async fn get_write_stream(&self, call: FakeCall) {
        let Some((call, request)) = self.first_request::<GetWriteStreamRequest>(call).await else {
            return;
        };
        let Some((call, table)) = self.write_table(call, &request.name) else {
            return;
        };
        let rpc = BigQueryFakeRpc::GetWriteStream;
        self.answer_write(call, rpc, &table, |state| {
            let stream_type = if request.name.ends_with(DEFAULT_STREAM) {
                WriteStreamType::Committed
            } else {
                state
                    .write_streams
                    .get(&BigQueryWriteStreamName::reported(request.name.clone()))
                    .ok_or_else(|| stream_not_found(&request.name))?
                    .stream_type()
            };
            let schema = &state
                .tables
                .get(&table)
                .ok_or_else(|| table.not_found())?
                .schema;
            Ok(write_stream(request.name.clone(), stream_type, schema))
        })
        .await;
    }

    async fn create_write_stream(&self, call: FakeCall) {
        let Some((call, request)) = self.first_request::<CreateWriteStreamRequest>(call).await
        else {
            return;
        };
        let Some((call, table)) = self.write_table(call, &request.parent) else {
            return;
        };
        let rpc = BigQueryFakeRpc::CreateWriteStream;
        self.answer_write(call, rpc, &table, |state| {
            let stream_type = request
                .write_stream
                .as_ref()
                .map_or(WriteStreamType::Unspecified, WriteStream::r#type);
            let mode = match stream_type {
                WriteStreamType::Committed => BigQueryWriteMode::Committed,
                WriteStreamType::Pending => BigQueryWriteMode::Pending,
                WriteStreamType::Buffered => BigQueryWriteMode::Buffered,
                WriteStreamType::Unspecified => {
                    return Err(Status::invalid_argument("the write stream has no type"))
                }
            };
            let schema = state
                .tables
                .get(&table)
                .ok_or_else(|| table.not_found())?
                .schema
                .clone();
            let name = format!("{}/streams/fake-stream-{}", table.path(), state.next_id());
            state.write_streams.insert(
                BigQueryWriteStreamName::reported(name.clone()),
                FakeWriteStream::new(table.clone(), mode),
            );
            Ok(write_stream(name, stream_type, &schema))
        })
        .await;
    }

    async fn flush_rows(&self, call: FakeCall) {
        let Some((call, request)) = self.first_request::<FlushRowsRequest>(call).await else {
            return;
        };
        let Some((call, table)) = self.write_table(call, &request.write_stream) else {
            return;
        };
        let rpc = BigQueryFakeRpc::FlushRows;
        self.answer_write(call, rpc, &table, |state| {
            let name = BigQueryWriteStreamName::reported(request.write_stream.clone());
            let stream = state
                .write_streams
                .get_mut(&name)
                .ok_or_else(|| stream_not_found(&request.write_stream))?;
            if stream.mode != BigQueryWriteMode::Buffered {
                return Err(Status::invalid_argument(format!(
                    "FlushRows is supported on BUFFERED streams only, and {name} is not one"
                )));
            }
            let offset = request
                .offset
                .ok_or_else(|| Status::invalid_argument("FlushRows needs an offset"))?;
            if !(0..stream.length).contains(&offset) {
                return Err(Status::out_of_range(format!(
                    "the offset {offset} is not a row of {name}, which has {} rows",
                    stream.length
                )));
            }
            let table = state
                .tables
                .get_mut(&table)
                .ok_or_else(|| table.not_found())?;
            table.batches.extend(stream.flush_to(offset));
            Ok(FlushRowsResponse { offset })
        })
        .await;
    }

    async fn finalize_write_stream(&self, call: FakeCall) {
        let Some((call, request)) = self.first_request::<FinalizeWriteStreamRequest>(call).await
        else {
            return;
        };
        let Some((call, table)) = self.write_table(call, &request.name) else {
            return;
        };
        let rpc = BigQueryFakeRpc::FinalizeWriteStream;
        self.answer_write(call, rpc, &table, |state| {
            let stream = state
                .write_streams
                .get_mut(&BigQueryWriteStreamName::reported(request.name.clone()))
                .ok_or_else(|| stream_not_found(&request.name))?;
            stream.finalized = true;
            Ok(FinalizeWriteStreamResponse {
                row_count: stream.length,
            })
        })
        .await;
    }

    async fn batch_commit_write_streams(&self, call: FakeCall) {
        let Some((call, request)) = self
            .first_request::<BatchCommitWriteStreamsRequest>(call)
            .await
        else {
            return;
        };
        let Some((call, table)) = self.write_table(call, &request.parent) else {
            return;
        };
        let rpc = BigQueryFakeRpc::BatchCommitWriteStreams;
        self.answer_write(call, rpc, &table, |state| {
            if !state.tables.contains_key(&table) {
                return Err(table.not_found());
            }
            let names: Vec<BigQueryWriteStreamName> = request
                .write_streams
                .iter()
                .map(|name| BigQueryWriteStreamName::reported(name.clone()))
                .collect();
            let stream_errors: Vec<StorageError> = names
                .iter()
                .filter_map(|name| {
                    let refused = match state.write_streams.get(name) {
                        Some(stream) => stream.commit_refusal(&table)?,
                        None => StorageErrorCode::StreamNotFound,
                    };
                    Some(StorageError {
                        code: refused.into(),
                        entity: name.to_string(),
                        error_message: format!("{name} cannot be committed: {refused:?}"),
                    })
                })
                .collect();
            if !stream_errors.is_empty() {
                return Ok(BatchCommitWriteStreamsResponse {
                    commit_time: None,
                    stream_errors,
                });
            }
            let mut committed = Vec::new();
            for name in &names {
                if let Some(stream) = state.write_streams.get_mut(name) {
                    stream.committed = true;
                    committed.extend(stream.appended.iter().cloned());
                }
            }
            if let Some(table) = state.tables.get_mut(&table) {
                table.batches.extend(committed);
            }
            let now = BigQueryInstant::now();
            Ok(BatchCommitWriteStreamsResponse {
                commit_time: Some(Timestamp {
                    seconds: now.as_second(),
                    nanos: now.subsec_nanosecond(),
                }),
                stream_errors: Vec::new(),
            })
        })
        .await;
    }

    /// Serves one `AppendRows` connection, answering its requests in order until the client
    /// closes its side.
    ///
    /// A fault answers the request that drew it: a status as that request's in-band error,
    /// with nothing written; a dropped connection after the request is written, as a response
    /// lost on its way back, so the client sends the request again.
    async fn append_rows(&self, mut call: FakeCall) {
        let mut connection = AppendConnection::default();
        loop {
            let request = match call.try_next_request::<AppendRowsRequest>().await {
                Ok(Some(request)) => request,
                Ok(None) => {
                    call.finish();
                    return;
                }
                Err(error) => {
                    let described = format!("AppendRows whose request does not decode: {error}");
                    self.unmatched(call, &described, &[]);
                    return;
                }
            };
            let Some((returned, table)) = self.write_table(call, &request.write_stream) else {
                return;
            };
            call = returned;
            connection.remember_writer_schema(&request);
            let fault = self
                .rules()
                .fault(BigQueryFakeRpc::AppendRows, Some(&table));
            let response = match fault {
                Some(BigQueryFakeFault::Status { code, message }) => {
                    in_band(plain_status(code.grpc_code(), message))
                }
                Some(fault @ BigQueryFakeFault::Hang) => {
                    fault.answer(call).await;
                    return;
                }
                Some(fault @ BigQueryFakeFault::ConnectionDropped) => {
                    if let Err(failure) = self.append(&connection, &table, &request) {
                        self.internal(call, &failure);
                        return;
                    }
                    fault.answer(call).await;
                    return;
                }
                None => match self.append(&connection, &table, &request) {
                    Ok(response) => response,
                    Err(failure) => {
                        self.internal(call, &failure);
                        return;
                    }
                },
            };
            call.send(&response);
        }
    }

    /// Writes the rows of one append request to `table` and returns its answer, which is an
    /// in-band error when the request is refused and nothing is written.
    ///
    /// # Errors
    /// An internal failure of the fake, such as a panicking `reject_rows` predicate.
    fn append(
        &self,
        connection: &AppendConnection,
        table: &TableKey,
        request: &AppendRowsRequest,
    ) -> Result<AppendRowsResponse, String> {
        let target = self.state().append_target(table, request);
        let (schema, layout) = match target {
            Ok(target) => (target.schema, target.layout),
            Err(refused) => return Ok(in_band(refused)),
        };
        let decoded = match connection.decode(&schema, &layout, request) {
            Ok(decoded) => decoded,
            Err(refused) => return Ok(in_band(plain_status(Code::InvalidArgument, refused))),
        };
        if let Some(refused) = self.reject_rows(table, &decoded.rows)? {
            return Ok(refused);
        }
        Ok(self.state().append(table, request, decoded))
    }

    /// The answer of the `reject_rows` rules of `table` to `rows`: row errors if any rule
    /// refuses a row, counting each rule that does, and `None` if none does. The predicates run
    /// outside the locks.
    ///
    /// # Errors
    /// A predicate that panicked, or rows that do not read as its type.
    fn reject_rows(
        &self,
        table: &TableKey,
        rows: &RecordBatch,
    ) -> Result<Option<AppendRowsResponse>, String> {
        let rules = self.rules().rejections(table);
        let mut row_errors: Vec<RowError> = Vec::new();
        for rule in rules {
            let refused = catch_unwind(AssertUnwindSafe(|| (rule.rejects)(rows)))
                .map_err(|_| format!("the reject_rows predicate on {table} panicked"))?
                .map_err(|err| {
                    format!("reject_rows on {table} cannot read an appended row: {err}")
                })?;
            let mut counted = false;
            for index in refused {
                let index = i64::try_from(index).map_err(|err| err.to_string())?;
                if row_errors.iter().any(|error| error.index == index) {
                    continue;
                }
                row_errors.push(RowError {
                    index,
                    code: RowErrorCode::FieldsError.into(),
                    message: rule.reason.clone(),
                });
                counted = true;
            }
            if counted {
                rule.rule.count();
            }
        }
        if row_errors.is_empty() {
            return Ok(None);
        }
        row_errors.sort_by_key(|error| error.index);
        Ok(Some(AppendRowsResponse {
            row_errors,
            ..in_band(plain_status(
                Code::InvalidArgument,
                "Errors found while processing rows.".to_string(),
            ))
        }))
    }
}

/// What an append request writes to, once the stream, the table and the offset are checked.
struct AppendTarget {
    schema: BigQueryTableSchema,
    /// The Arrow layout of `schema`, which appended Arrow rows are matched to by name.
    layout: SchemaRef,
    /// Where the request's rows start in a stream with offsets; `None` for the default
    /// stream.
    offset: Option<i64>,
}

impl FakeState {
    /// Checks that `request` may append to its stream of `table` now.
    ///
    /// # Errors
    /// The status of the in-band error BigQuery answers a refused request with: a stream that
    /// does not exist or no longer takes rows, a missing table, an offset on the default
    /// stream, or an offset other than the stream's length.
    fn append_target(
        &self,
        table: &TableKey,
        request: &AppendRowsRequest,
    ) -> Result<AppendTarget, rpc::Status> {
        let name = &request.write_stream;
        let offset = if name.ends_with(DEFAULT_STREAM) {
            if let Some(offset) = request.offset {
                return Err(plain_status(
                    Code::InvalidArgument,
                    format!("the default stream {name} takes no offset, got {offset}"),
                ));
            }
            None
        } else {
            let stream = self
                .write_streams
                .get(&BigQueryWriteStreamName::reported(name.clone()))
                .ok_or_else(|| {
                    storage_status(Code::NotFound, StorageErrorCode::StreamNotFound, name)
                })?;
            if stream.finalized || stream.committed {
                return Err(storage_status(
                    Code::InvalidArgument,
                    StorageErrorCode::StreamFinalized,
                    name,
                ));
            }
            match request.offset {
                Some(offset) if offset < stream.length => {
                    return Err(storage_status(
                        Code::AlreadyExists,
                        StorageErrorCode::OffsetAlreadyExists,
                        name,
                    ))
                }
                Some(offset) if offset > stream.length => {
                    return Err(storage_status(
                        Code::OutOfRange,
                        StorageErrorCode::OffsetOutOfRange,
                        name,
                    ))
                }
                _ => Some(stream.length),
            }
        };
        let fake_table = self.tables.get(table).ok_or_else(|| {
            storage_status(
                Code::NotFound,
                StorageErrorCode::TableNotFound,
                &table.path(),
            )
        })?;
        Ok(AppendTarget {
            schema: fake_table.schema.clone(),
            layout: fake_table.arrow_schema.clone(),
            offset,
        })
    }

    /// Writes `decoded`, the rows of `request`, to its stream of `table`, checking the stream
    /// again, and returns the request's answer.
    fn append(
        &mut self,
        table: &TableKey,
        request: &AppendRowsRequest,
        decoded: DecodedRows,
    ) -> AppendRowsResponse {
        let offset = match self.append_target(table, request) {
            Ok(target) => target.offset,
            Err(refused) => return in_band(refused),
        };
        let DecodedRows { rows, changes } = decoded;
        let written = i64::try_from(rows.num_rows()).unwrap_or(i64::MAX);
        let name = BigQueryWriteStreamName::reported(request.write_stream.clone());
        let visible = match (self.write_streams.get_mut(&name), changes) {
            (None, Some(changes)) => {
                if let Some(fake_table) = self.tables.get_mut(table) {
                    fake_table.changes.push(FakeChanges { rows, changes });
                }
                None
            }
            (None, None) => Some(rows),
            (Some(_), Some(_)) => {
                return in_band(plain_status(
                    Code::InvalidArgument,
                    format!("CDC rows go through the default stream only, not {name}"),
                ))
            }
            (Some(stream), None) => {
                stream.appended.push(rows.clone());
                stream.length += written;
                (stream.mode == BigQueryWriteMode::Committed).then_some(rows)
            }
        };
        if let (Some(rows), Some(fake_table)) = (visible, self.tables.get_mut(table)) {
            fake_table.batches.push(rows);
        }
        AppendRowsResponse {
            response: Some(Response::AppendResult(AppendResult { offset })),
            ..Default::default()
        }
    }
}

impl FakeWriteStream {
    /// The type this stream reports itself as.
    fn stream_type(&self) -> WriteStreamType {
        match self.mode {
            BigQueryWriteMode::Pending => WriteStreamType::Pending,
            BigQueryWriteMode::Buffered => WriteStreamType::Buffered,
            _ => WriteStreamType::Committed,
        }
    }

    /// Makes the rows up to and including `offset` visible, and returns the rows no earlier
    /// flush made visible. `flushed` counts the rows made visible so far.
    fn flush_to(&mut self, offset: i64) -> Vec<RecordBatch> {
        let end = offset + 1;
        let mut newly_visible = Vec::new();
        let mut first_row = 0;
        for batch in &self.appended {
            let rows = i64::try_from(batch.num_rows()).unwrap_or(i64::MAX);
            let start = self.flushed.max(first_row);
            let stop = end.min(first_row + rows);
            if start < stop {
                let from = usize::try_from(start - first_row).unwrap_or_default();
                let length = usize::try_from(stop - start).unwrap_or_default();
                newly_visible.push(batch.slice(from, length));
            }
            first_row += rows;
        }
        self.flushed = self.flushed.max(end);
        newly_visible
    }

    /// Why this stream cannot be committed to `table`, or `None` if it can.
    fn commit_refusal(&self, table: &TableKey) -> Option<StorageErrorCode> {
        if self.table != *table {
            Some(StorageErrorCode::StreamNotFound)
        } else if self.mode != BigQueryWriteMode::Pending {
            Some(StorageErrorCode::InvalidStreamType)
        } else if self.committed {
            Some(StorageErrorCode::StreamAlreadyCommitted)
        } else if !self.finalized {
            Some(StorageErrorCode::InvalidStreamState)
        } else {
            None
        }
    }
}

/// The writer schema of an `AppendRows` connection. BigQuery reads it from the requests that
/// carry it, and holds it for the requests after them that do not.
#[derive(Default)]
struct AppendConnection {
    proto: Option<DescriptorProto>,
    /// The IPC schema message of Arrow rows.
    arrow: Option<Vec<u8>>,
}

impl AppendConnection {
    fn remember_writer_schema(&mut self, request: &AppendRowsRequest) {
        match &request.rows {
            Some(Rows::ProtoRows(data)) => {
                if let Some(descriptor) = data
                    .writer_schema
                    .as_ref()
                    .and_then(|schema| schema.proto_descriptor.clone())
                {
                    self.proto = Some(descriptor);
                }
            }
            Some(Rows::ArrowRows(data)) => {
                if let Some(schema) = &data.writer_schema {
                    self.arrow = Some(schema.serialized_schema.clone());
                }
            }
            None => {}
        }
    }

    /// The rows of `request`, in `layout`, the read layout of `schema`.
    ///
    /// # Errors
    /// Why BigQuery would refuse the rows: no writer schema, rows that do not decode by it,
    /// or columns that do not match the table's.
    fn decode(
        &self,
        schema: &BigQueryTableSchema,
        layout: &SchemaRef,
        request: &AppendRowsRequest,
    ) -> Result<DecodedRows, String> {
        match &request.rows {
            Some(Rows::ProtoRows(data)) => {
                let descriptor = self
                    .proto
                    .as_ref()
                    .ok_or("the connection has sent no proto writer schema")?;
                let mut builder = ProtoBatchBuilder::from_descriptor(schema, descriptor)
                    .map_err(|err| err.to_string())?;
                let rows = data.rows.as_ref().map(|rows| &rows.serialized_rows);
                for (index, row) in rows.into_iter().flatten().enumerate() {
                    builder
                        .push(row)
                        .map_err(|err| format!("row {index}: {err}"))?;
                }
                builder.finish().map_err(|err| err.to_string())
            }
            Some(Rows::ArrowRows(data)) => {
                let ipc_schema = self
                    .arrow
                    .as_ref()
                    .ok_or("the connection has sent no Arrow writer schema")?;
                let record_batch = data
                    .rows
                    .as_ref()
                    .ok_or("the request carries no record batch")?;
                let batch = ArrowIpcDecoder::new(ipc_schema)
                    .and_then(|mut decoder| decoder.decode(&record_batch.serialized_record_batch))
                    .map_err(|err| err.to_string())?;
                Ok(DecodedRows {
                    rows: arrow_rows(&batch, layout)?,
                    changes: None,
                })
            }
            None => Err("the request carries no rows".to_string()),
        }
    }
}

/// `batch` in `layout`, its columns matched to the table's by name, ignoring case as BigQuery
/// does. A column the batch leaves out is NULL.
///
/// # Errors
/// A column the table does not have, one whose Arrow type is not the table's, or a REQUIRED
/// column with NULLs or left out.
fn arrow_rows(batch: &RecordBatch, layout: &SchemaRef) -> Result<RecordBatch, String> {
    let batch_schema = batch.schema();
    if let Some(extra) = batch_schema.fields().iter().find(|field| {
        !layout
            .fields()
            .iter()
            .any(|column| column.name().eq_ignore_ascii_case(field.name()))
    }) {
        return Err(format!(
            "the table has no column for the Arrow field {}",
            extra.name()
        ));
    }
    let columns = layout
        .fields()
        .iter()
        .map(|column| {
            let found = batch_schema
                .fields()
                .iter()
                .position(|field| field.name().eq_ignore_ascii_case(column.name()));
            match found {
                Some(index) if batch.column(index).data_type() == column.data_type() => {
                    Ok(batch.column(index).clone())
                }
                Some(index) => Err(format!(
                    "the Arrow field {} is {}, and its column takes {}",
                    column.name(),
                    batch.column(index).data_type(),
                    column.data_type()
                )),
                None => Ok(new_null_array(column.data_type(), batch.num_rows())),
            }
        })
        .collect::<Result<Vec<_>, String>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(layout.clone(), columns, &options)
        .map_err(|err| format!("the Arrow rows do not fit the table: {err}"))
}

/// The `WriteStream` the stream RPCs answer with.
fn write_stream(
    name: String,
    stream_type: WriteStreamType,
    schema: &BigQueryTableSchema,
) -> WriteStream {
    WriteStream {
        name,
        r#type: stream_type.into(),
        table_schema: Some(storage::TableSchema::from(schema)),
        write_mode: WriteMode::Insert.into(),
        ..Default::default()
    }
}

/// The `NotFound` BigQuery answers for a write stream that does not exist.
fn stream_not_found(name: &str) -> Status {
    Status::not_found(format!("Requested entity was not found. Entity: {name}"))
}

fn plain_status(code: Code, message: String) -> rpc::Status {
    rpc::Status {
        code: code as i32,
        message,
        details: Vec::new(),
    }
}

/// A status with a `StorageError` naming `entity`.
fn storage_status(code: Code, storage: StorageErrorCode, entity: &str) -> rpc::Status {
    let message = format!("{}: {entity}", storage.as_str_name());
    storage_error_status(code, storage, entity, &message)
}

fn in_band(status: rpc::Status) -> AppendRowsResponse {
    AppendRowsResponse {
        response: Some(Response::Error(status)),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use crate::errors::BigQueryError;
    use crate::testing::{BigQueryFake, BigQueryFakeCode, BigQueryFakeFault, BigQueryFakeRpc};
    use crate::{
        BigQueryChange, BigQueryChangeType, BigQueryDatasetId, BigQueryResult,
        BigQueryStreamingWriteOptions, BigQueryTableId, BigQueryWriteMode,
    };
    use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Order {
        id: i64,
        customer: String,
        total: f64,
    }

    const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
    const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");

    fn order(id: i64, customer: &str, total: f64) -> Order {
        Order {
            id,
            customer: customer.to_string(),
            total,
        }
    }

    fn orders() -> Vec<Order> {
        vec![
            order(1, "Alice", 120.0),
            order(2, "Bob", 80.5),
            order(3, "Carol", 7.25),
        ]
    }

    async fn fake_with_orders() -> BigQueryResult<BigQueryFake> {
        let fake = BigQueryFake::start().await?;
        fake.table(SHOP.table(ORDERS), |columns| columns.from_type::<Order>())
            .create()?;
        Ok(fake)
    }

    fn options(mode: BigQueryWriteMode) -> BigQueryStreamingWriteOptions {
        BigQueryStreamingWriteOptions::new().with_mode(mode)
    }

    #[tokio::test]
    async fn exactly_once_keeps_one_copy_of_a_request_sent_again() -> BigQueryResult<()> {
        let fake = fake_with_orders().await?;
        let lost = fake
            .fault(BigQueryFakeRpc::AppendRows)
            .times(1)
            .respond(BigQueryFakeFault::ConnectionDropped)?;

        let summary = fake
            .db()
            .fluent()
            .insert()
            .into(SHOP.table(ORDERS))
            .objects(orders())
            .exactly_once()
            .execute()
            .await?;

        assert_eq!(summary.rows_written, 3);
        assert_eq!(fake.rows::<Order>(SHOP.table(ORDERS))?, orders());
        assert_eq!(lost.calls(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn buffered_rows_show_up_to_the_flushed_offset() -> BigQueryResult<()> {
        let fake = fake_with_orders().await?;
        let (mut writer, _responses) = fake
            .db()
            .create_streaming_writer_with_options::<Order>(
                SHOP.table(ORDERS),
                options(BigQueryWriteMode::Buffered),
            )
            .await?;
        writer.write_all(&orders()).await?;
        writer.flush().await?;
        assert_eq!(fake.rows::<Order>(SHOP.table(ORDERS))?, Vec::new());

        assert_eq!(writer.flush_rows_to(1).await?, 1);
        assert_eq!(fake.rows::<Order>(SHOP.table(ORDERS))?, orders()[..2]);

        writer.finish().await?;
        assert_eq!(fake.rows::<Order>(SHOP.table(ORDERS))?, orders());
        Ok(())
    }

    #[tokio::test]
    async fn atomic_rows_show_only_at_the_commit() -> BigQueryResult<()> {
        let fake = fake_with_orders().await?;
        let (mut writer, _responses) = fake
            .db()
            .create_streaming_writer_with_options::<Order>(
                SHOP.table(ORDERS),
                options(BigQueryWriteMode::Pending),
            )
            .await?;
        writer.write_all(&orders()).await?;
        let finalized = writer.finalize().await?;
        assert_eq!(finalized.row_count, 3);
        assert_eq!(fake.rows::<Order>(SHOP.table(ORDERS))?, Vec::new());

        fake.db().commit_write_streams(vec![finalized]).await?;

        assert_eq!(fake.rows::<Order>(SHOP.table(ORDERS))?, orders());
        Ok(())
    }

    #[tokio::test]
    async fn changes_are_recorded_and_not_applied() -> BigQueryResult<()> {
        let fake = fake_with_orders().await?;
        let changes = vec![
            BigQueryChange {
                change_type: BigQueryChangeType::Upsert,
                sequence_number: Some(1.into()),
                row: order(1, "Alice", 120.0),
            },
            BigQueryChange {
                change_type: BigQueryChangeType::Delete,
                sequence_number: None,
                row: order(2, "Bob", 80.5),
            },
        ];

        fake.db()
            .fluent()
            .insert()
            .into(SHOP.table(ORDERS))
            .changes(changes.clone())
            .execute()
            .await?;

        assert_eq!(fake.changes::<Order>(SHOP.table(ORDERS))?, changes);
        assert_eq!(fake.rows::<Order>(SHOP.table(ORDERS))?, Vec::new());
        Ok(())
    }

    #[tokio::test]
    async fn arrow_columns_land_by_name() -> BigQueryResult<()> {
        let fake = fake_with_orders().await?;
        let columns: Vec<(&str, ArrayRef)> = vec![
            ("total", Arc::new(Float64Array::from(vec![120.0, 80.5]))),
            (
                "customer",
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            ),
            ("id", Arc::new(Int64Array::from(vec![1, 2]))),
        ];
        let batch = RecordBatch::try_from_iter(columns).expect("columns of equal length");

        fake.db()
            .fluent()
            .insert()
            .into(SHOP.table(ORDERS))
            .record_batches(vec![batch])
            .execute()
            .await?;

        assert_eq!(fake.rows::<Order>(SHOP.table(ORDERS))?, orders()[..2]);
        Ok(())
    }

    #[tokio::test]
    async fn rejected_rows_fail_their_request_and_write_nothing() -> BigQueryResult<()> {
        let fake = fake_with_orders().await?;
        let rejected = fake.reject_rows(SHOP.table(ORDERS), "customer is banned", |row: &Order| {
            row.customer == "Bob"
        });

        let written = fake
            .db()
            .fluent()
            .insert()
            .into(SHOP.table(ORDERS))
            .objects(orders())
            .execute()
            .await;

        match written {
            Err(BigQueryError::RowErrors(errors)) => {
                let rows: Vec<u64> = errors.errors.iter().map(|error| error.row).collect();
                assert_eq!(rows, vec![1]);
                assert_eq!(errors.errors[0].message, "customer is banned");
            }
            other => panic!("expected row errors, got {other:?}"),
        }
        assert_eq!(fake.rows::<Order>(SHOP.table(ORDERS))?, Vec::new());
        assert_eq!(rejected.calls(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn an_append_fault_is_retried() -> BigQueryResult<()> {
        let fake = fake_with_orders().await?;
        let aborted =
            fake.fault(BigQueryFakeRpc::AppendRows)
                .times(1)
                .respond(BigQueryFakeFault::status(
                    BigQueryFakeCode::Aborted,
                    "transaction aborted",
                ))?;

        fake.db()
            .fluent()
            .insert()
            .into(SHOP.table(ORDERS))
            .objects(orders())
            .execute()
            .await?;

        assert_eq!(fake.rows::<Order>(SHOP.table(ORDERS))?, orders());
        assert_eq!(aborted.calls(), 1);
        Ok(())
    }
}
