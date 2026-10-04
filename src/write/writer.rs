//! Streaming writers over the Storage Write API.

use crate::errors::{BigQueryCodecErrorKind, BigQueryError};
use crate::types::error::CodecError;
use crate::write::batch::{Batcher, RequestTarget, MAX_REQUEST_BYTES};
use crate::write::connection::{
    Command, ConnectionTask, FinishKind, Finished, Shared, TaskSettings, TASK_ENDED,
};
use crate::write::descriptor::{relaxes_a_required_field, WritePlan};
use crate::write::encoder::Encoder;
use crate::{
    BigQueryDb, BigQueryFinalizedStream, BigQueryResult, BigQueryStreamingWriteOptions,
    BigQueryTableRef, BigQueryTableSchema, BigQueryWriteMode, BigQueryWriteResponse,
    BigQueryWriteSummary,
};
use crate::{BigQueryInstant, BigQueryWriteStreamName};
use futures::stream::BoxStream;
use futures::StreamExt;
use gcloud_sdk::google::cloud::bigquery::storage::v1::write_stream::Type as WriteStreamType;
use gcloud_sdk::google::cloud::bigquery::storage::v1::{
    BatchCommitWriteStreamsRequest, CreateWriteStreamRequest, FinalizeWriteStreamRequest,
    GetWriteStreamRequest, WriteStream, WriteStreamView,
};
use serde::Serialize;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::field::Empty;
use tracing::{debug_span, warn, Span};

/// The schema a write stream reports.
///
/// # Errors
/// [`BigQueryError::WriteStreamError`] with code `NO_TABLE_SCHEMA` for a stream that came back
/// without one, and the errors of the schema conversion.
impl TryFrom<&WriteStream> for BigQueryTableSchema {
    type Error = BigQueryError;

    fn try_from(stream: &WriteStream) -> Result<Self, Self::Error> {
        let schema = stream.table_schema.as_ref().ok_or_else(|| {
            BigQueryError::write_stream(
                "NO_TABLE_SCHEMA",
                stream.name.clone(),
                "the write stream came back without its table schema",
            )
        })?;
        BigQueryTableSchema::try_from(schema)
    }
}

impl BigQueryDb {
    pub(crate) async fn get_write_stream(
        &self,
        span: &Span,
        name: &str,
    ) -> BigQueryResult<WriteStream> {
        let request = GetWriteStreamRequest {
            name: name.to_string(),
            view: WriteStreamView::Full.into(),
        };
        self.retry(span, "get the write stream", &request, |request| {
            let mut client = self.write_client();
            async move { client.get_write_stream(request).await }
        })
        .await
    }

    async fn create_write_stream(
        &self,
        span: &Span,
        table_path: &str,
        stream_type: WriteStreamType,
    ) -> BigQueryResult<WriteStream> {
        let request = CreateWriteStreamRequest {
            parent: table_path.to_string(),
            write_stream: Some(WriteStream {
                r#type: stream_type.into(),
                ..Default::default()
            }),
        };
        self.retry(span, "create a write stream", &request, |request| {
            let mut client = self.write_client();
            async move { client.create_write_stream(request).await }
        })
        .await
    }

    /// Finalizes a committed or pending stream and returns the rows it holds.
    pub(crate) async fn finalize_write_stream(
        &self,
        span: &Span,
        name: &str,
    ) -> BigQueryResult<i64> {
        let request = FinalizeWriteStreamRequest {
            name: name.to_string(),
        };
        let response = self
            .retry(span, "finalize the write stream", &request, |request| {
                let mut client = self.write_client();
                async move { client.finalize_write_stream(request).await }
            })
            .await?;
        Ok(response.row_count)
    }

    /// Commits finalized pending streams of the table at `table_path` together.
    pub(crate) async fn batch_commit(
        &self,
        span: &Span,
        table_path: &str,
        streams: Vec<String>,
    ) -> BigQueryResult<BigQueryInstant> {
        let request = BatchCommitWriteStreamsRequest {
            parent: table_path.to_string(),
            write_streams: streams,
        };
        let response = self
            .retry(span, "commit the write streams", &request, |request| {
                let mut client = self.write_client();
                async move { client.batch_commit_write_streams(request).await }
            })
            .await?;
        if let Some(error) = response.stream_errors.first() {
            let code = gcloud_sdk::google::cloud::bigquery::storage::v1::storage_error::StorageErrorCode::try_from(error.code)
            .map(|code| code.as_str_name().to_string())
            .unwrap_or_else(|_| error.code.to_string());
            return Err(BigQueryError::write_stream(
                &code,
                error.entity.clone(),
                error.error_message.clone(),
            ));
        }
        let commit_time = response.commit_time.ok_or_else(|| {
            BigQueryError::write_stream(
                "NO_COMMIT_TIME",
                request.write_streams.join(", "),
                "the commit reported no error and no commit time",
            )
        })?;
        BigQueryInstant::new(commit_time.seconds, commit_time.nanos).map_err(|err| {
            BigQueryError::write_stream(
                "INVALID_COMMIT_TIME",
                request.write_streams.join(", "),
                format!("the commit time {commit_time:?} is not a timestamp: {err}"),
            )
        })
    }
}

impl BigQueryStreamingWriteOptions {
    fn check(&self, cdc: bool) -> BigQueryResult<()> {
        if self.max_request_bytes == 0 || self.max_request_bytes > MAX_REQUEST_BYTES {
            return Err(BigQueryError::invalid_parameters(
                "max_request_bytes",
                format!(
                "{} is outside 1..={MAX_REQUEST_BYTES}; BigQuery ends the whole connection on a \
                 request over about 20 MB",
                self.max_request_bytes
            ),
            ));
        }
        if self.max_batch_rows == Some(0) {
            return Err(BigQueryError::invalid_parameters(
                "max_batch_rows",
                "a batch holds at least one row",
            ));
        }
        if self.max_inflight_requests == 0 {
            return Err(BigQueryError::invalid_parameters(
                "max_inflight_requests",
                "at least one request must be allowed in flight",
            ));
        }
        if cdc && self.mode != BigQueryWriteMode::Default {
            return Err(BigQueryError::invalid_parameters(
                "mode",
                format!(
                    "CDC writes go through the default stream only, not {:?}",
                    self.mode
                ),
            ));
        }
        Ok(())
    }
}

impl BigQueryCodecErrorKind {
    /// Whether a failed encode can be caused by a schema the writer has not seen yet: a column
    /// added, or a REQUIRED column relaxed to NULLABLE.
    fn may_be_a_stale_schema(self) -> bool {
        matches!(
            self,
            BigQueryCodecErrorKind::UnknownField
                | BigQueryCodecErrorKind::NullForRequired
                | BigQueryCodecErrorKind::MissingRequiredField
        )
    }
}

/// The untyped writer behind [`BigQueryStreamingWriter`] and
/// [`BigQueryCdcWriter`](crate::BigQueryCdcWriter).
pub(crate) struct WriterCore {
    db: BigQueryDb,
    span: Span,
    table: BigQueryTableRef,
    stream: String,
    mode: BigQueryWriteMode,
    cdc: bool,
    refresh_interval: Duration,
    last_refresh: Option<Instant>,
    encoder: Encoder,
    row_hint: usize,
    shared: Arc<Shared>,
    commands: mpsc::UnboundedSender<Command>,
    task: Option<JoinHandle<()>>,
    finished: bool,
}

pub(crate) type ResponseStream<'b> = BoxStream<'b, BigQueryResult<BigQueryWriteResponse>>;

impl WriterCore {
    pub(crate) async fn open<'b>(
        db: &BigQueryDb,
        table: BigQueryTableRef,
        options: BigQueryStreamingWriteOptions,
        cdc: bool,
    ) -> BigQueryResult<(Self, ResponseStream<'b>)> {
        options.check(cdc)?;
        let table_path = table.table_path(&db.options().google_project_id);
        let span = debug_span!(
            "BigQuery streaming write",
            "/bigquery/table" = %table,
            "/bigquery/write_mode" = ?options.mode,
            "/bigquery/rows_appended" = Empty,
            "/bigquery/bytes_sent" = Empty,
            "/bigquery/appends" = Empty,
            "/bigquery/retries" = Empty,
        );
        let stream = match options.mode {
            BigQueryWriteMode::Default => {
                db.get_write_stream(&span, &format!("{table_path}/streams/_default"))
                    .await?
            }
            BigQueryWriteMode::Committed => {
                db.create_write_stream(&span, &table_path, WriteStreamType::Committed)
                    .await?
            }
            BigQueryWriteMode::Pending => {
                db.create_write_stream(&span, &table_path, WriteStreamType::Pending)
                    .await?
            }
        };
        let schema = BigQueryTableSchema::try_from(&stream)?;
        let plan = Arc::new(WritePlan::new(&schema, cdc));
        let target = RequestTarget {
            write_stream: stream.name.clone(),
            trace_id: options.trace_id.as_ref().map(ToString::to_string),
            missing_value: options.missing_value,
        };
        let batcher = Batcher::new(
            plan.clone(),
            &target,
            options.max_request_bytes,
            options.max_batch_rows,
        );
        let shared = Arc::new(Shared::new(
            batcher,
            options.max_inflight_requests,
            options.max_inflight_bytes,
        ));
        let (commands, commands_rx) = mpsc::unbounded_channel();
        let (responses, responses_rx) = futures::channel::mpsc::unbounded();
        let task = ConnectionTask::new(
            TaskSettings {
                db: db.clone(),
                span: span.clone(),
                mode: options.mode,
                target,
                table: table.clone(),
                table_path,
                batch_delay: options.max_batch_delay,
            },
            shared.clone(),
            commands_rx,
            responses,
        );
        let task = tokio::spawn(task.run());
        let core = WriterCore {
            db: db.clone(),
            span,
            table,
            stream: stream.name,
            mode: options.mode,
            cdc,
            refresh_interval: options.schema_refresh_interval,
            last_refresh: None,
            encoder: Encoder::new(plan),
            row_hint: 0,
            shared,
            commands,
            task: Some(task),
            finished: false,
        };
        Ok((core, responses_rx.boxed()))
    }

    pub(crate) fn mode(&self) -> BigQueryWriteMode {
        self.mode
    }

    pub(crate) fn stream_name(&self) -> &str {
        &self.stream
    }

    fn check_failed(&self) -> BigQueryResult<()> {
        match &self.shared.lock().failed {
            Some(err) => Err(err.clone()),
            None => Ok(()),
        }
    }

    fn command(&self, command: Command) -> BigQueryResult<()> {
        self.commands
            .send(command)
            .map_err(|_| BigQueryError::system("WRITER_TASK_ENDED", TASK_ENDED))
    }

    /// Encodes one row with `encode` and adds it to the open batch. A row that fails because
    /// the writer's schema may be stale makes it read the schema again, at most once per
    /// `schema_refresh_interval`, and encode the row once more.
    pub(crate) async fn write_with<F>(&mut self, mut encode: F) -> BigQueryResult<()>
    where
        F: FnMut(&mut Encoder, &mut Vec<u8>) -> Result<(), CodecError>,
    {
        self.check_failed()?;
        let pending = self.shared.lock().pending_schema.take();
        if let Some(schema) = pending {
            self.switch_plan(schema).await?;
        }
        let index = self.shared.lock().batcher.next_row();
        let mut row = Vec::with_capacity(self.row_hint);
        if let Err(err) = encode(&mut self.encoder, &mut row) {
            if !(err.kind()).may_be_a_stale_schema() || !self.refresh_schema().await? {
                return Err(err.with_row(index).into_serialize());
            }
            row.clear();
            encode(&mut self.encoder, &mut row)
                .map_err(|err| err.with_row(index).into_serialize())?;
        }
        self.row_hint = row.len();
        let cost = self
            .shared
            .lock()
            .batcher
            .cost(row.len())
            .map_err(|err| err.with_row(index).into_serialize())?;
        if !self.shared.lock().batcher.fits(cost) {
            self.seal_open().await?;
        }
        let (opened, full) = {
            let mut state = self.shared.lock();
            if let Some(err) = &state.failed {
                return Err(err.clone());
            }
            let opened = state.batcher.push(row, cost, Instant::now());
            (opened, state.batcher.is_full())
        };
        if opened {
            self.command(Command::Wake)?;
        }
        if full {
            self.seal_open().await?;
        }
        Ok(())
    }

    /// Reads the stream's schema again unless it was read within `schema_refresh_interval`.
    /// Returns whether the plan changed.
    async fn refresh_schema(&mut self) -> BigQueryResult<bool> {
        let now = Instant::now();
        if self
            .last_refresh
            .is_some_and(|last| now < last + self.refresh_interval)
        {
            return Ok(false);
        }
        self.last_refresh = Some(now);
        let stream = self.db.get_write_stream(&self.span, &self.stream).await?;
        let schema = BigQueryTableSchema::try_from(&stream)?;
        self.switch_plan(schema).await
    }

    /// Encodes the next rows against `schema`, at a batch boundary. Returns whether the plan
    /// changed.
    async fn switch_plan(&mut self, schema: BigQueryTableSchema) -> BigQueryResult<bool> {
        if &schema == self.encoder.plan().schema() {
            return Ok(false);
        }
        self.seal_open().await?;
        let relaxed =
            relaxes_a_required_field(&self.encoder.plan().schema().fields, &schema.fields);
        let plan = Arc::new(WritePlan::new(&schema, self.cdc));
        self.shared.lock().batcher.set_plan(plan.clone());
        self.encoder = Encoder::new(plan);
        if relaxed {
            self.command(Command::Reconnect)?;
        }
        Ok(true)
    }

    /// Seals the open batch, waiting while the in-flight limits are reached.
    async fn seal_open(&self) -> BigQueryResult<()> {
        loop {
            let notified = self.shared.capacity.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.shared.lock();
                if let Some(err) = &state.failed {
                    return Err(err.clone());
                }
                if state.batcher.open_bytes().is_none() {
                    return Ok(());
                }
                if self.shared.can_seal(&state) {
                    self.shared.seal_locked(&mut state);
                    drop(state);
                    return self.command(Command::Wake);
                }
            }
            notified.await;
        }
    }

    pub(crate) async fn flush(&mut self) -> BigQueryResult<()> {
        self.seal_open().await?;
        let (tx, rx) = oneshot::channel();
        self.command(Command::Flush(tx))?;
        rx.await
            .map_err(|_| BigQueryError::system("WRITER_TASK_ENDED", TASK_ENDED))?
    }

    pub(crate) async fn finish(&mut self, kind: FinishKind) -> BigQueryResult<Finished> {
        self.finished = true;
        let sealed = self.seal_open().await;
        let (tx, rx) = oneshot::channel();
        self.command(Command::Finish(kind, tx))?;
        let finished = rx
            .await
            .map_err(|_| BigQueryError::system("WRITER_TASK_ENDED", TASK_ENDED))?;
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        sealed?;
        finished
    }

    /// Stops the writer without the drop warning, for callers that report their own error.
    pub(crate) fn abandon(&mut self) {
        self.finished = true;
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }

    pub(crate) fn table(&self) -> &BigQueryTableRef {
        &self.table
    }
}

impl Drop for WriterCore {
    fn drop(&mut self) {
        if !self.finished {
            warn!(
                parent: &self.span,
                stream = %self.stream,
                "A BigQuery streaming writer was dropped without finish(); rows not yet \
                 acknowledged may be lost and a pending stream stays uncommitted."
            );
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// A writer that streams rows of `T` into one table over one `AppendRows` connection.
///
/// Rows are encoded as they are written and sent in batches under `max_request_bytes`, as
/// configured in [`BigQueryStreamingWriteOptions`]. A background task owns the connection, so
/// the writer is `Send` but not `Clone`; open more writers for more connections. It holds no
/// `T`, so `T` needs neither `Send` nor `'static`.
///
/// The response stream returned with it yields one item per batch, in batch order, and ends
/// when the writer finishes or fails for good. Reading it is optional: [`finish`] reports
/// failed rows either way.
///
/// Call [`finish`] (or [`finalize`] in pending mode) when done. Dropping the writer logs a
/// warning, drops rows not yet acknowledged, and leaves a pending stream uncommitted.
///
/// The writer picks up schema changes: an `updated_schema` from BigQuery, or a field with no
/// column, makes it encode the next batches against the new schema. A dropped column is not
/// seen in time: BigQuery accepts its values and drops them silently for about 9 seconds
/// before it rejects the rows, so drop a column only after every writer has stopped sending it.
///
/// [`finish`]: BigQueryStreamingWriter::finish
/// [`finalize`]: BigQueryStreamingWriter::finalize
pub struct BigQueryStreamingWriter<T> {
    core: WriterCore,
    _row: PhantomData<fn(&T)>,
}

impl<T: Serialize> BigQueryStreamingWriter<T> {
    /// Writes one row. It waits while `max_inflight_requests` or `max_inflight_bytes` are
    /// unacknowledged.
    ///
    /// # Errors
    /// [`BigQueryError::SerializeError`] for a row that does not fit the table schema, naming
    /// the row by its index in write order; nothing of it is sent and the writer goes on. Once
    /// the writer has failed for good, that error, on every call.
    pub async fn write(&mut self, row: &T) -> BigQueryResult<()> {
        self.core
            .write_with(|encoder, out| encoder.encode(row, out))
            .await
    }

    /// Writes every row in order, stopping at the first error, as [`write`](Self::write).
    pub async fn write_all<'r, I>(&mut self, rows: I) -> BigQueryResult<()>
    where
        I: IntoIterator<Item = &'r T>,
        T: 'r,
    {
        for row in rows {
            self.write(row).await?;
        }
        Ok(())
    }

    /// Sends the open batch and waits until every batch written so far has an outcome.
    ///
    /// # Errors
    /// The writer's failure, if it failed for good. Failed batches are reported on the
    /// response stream and in the summary, not here.
    pub async fn flush(&mut self) -> BigQueryResult<()> {
        self.core.flush().await
    }

    /// Flushes, waits for every acknowledgement, and closes the stream: the default stream is
    /// left as it is, a committed one is finalized, and a pending one is finalized and
    /// committed.
    ///
    /// # Errors
    /// The writer's failure, if it failed for good. In pending mode,
    /// [`BigQueryError::WriteStreamError`] with code `NOT_COMMITTED` if any batch failed,
    /// since then nothing is committed.
    pub async fn finish(mut self) -> BigQueryResult<BigQueryWriteSummary> {
        Ok(self.core.finish(FinishKind::Close).await?.summary)
    }

    /// Pending mode only: flushes, waits for every acknowledgement and finalizes the stream,
    /// leaving the commit to [`BigQueryDb::commit_write_streams`], so several writers can
    /// commit together.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] in any other mode, and the errors of
    /// [`finish`](Self::finish).
    pub async fn finalize(mut self) -> BigQueryResult<BigQueryFinalizedStream> {
        if self.core.mode() != BigQueryWriteMode::Pending {
            let mode = self.core.mode();
            self.core.abandon();
            return Err(BigQueryError::invalid_parameters(
                "mode",
                format!("finalize() is for pending streams, the writer is {mode:?}"),
            ));
        }
        let finished = self.core.finish(FinishKind::Finalize).await?;
        Ok(BigQueryFinalizedStream {
            table: self.core.table().clone(),
            name: BigQueryWriteStreamName::reported(self.core.stream_name().to_string()),
            row_count: finished.finalized_rows.unwrap_or_default(),
        })
    }

    /// The write stream's name; `None` for the default stream.
    pub fn stream_name(&self) -> Option<&str> {
        (self.core.mode() != BigQueryWriteMode::Default).then(|| self.core.stream_name())
    }
}

impl BigQueryDb {
    /// Opens a streaming writer on `table` with default options: the default stream, at
    /// least once.
    ///
    /// Opening costs one `GetWriteStream` round trip, about 300 ms, so many small writes
    /// should share one writer.
    ///
    /// # Errors
    /// The `GetWriteStream` failure, such as [`BigQueryError::DataNotFoundError`] for a missing
    /// table.
    pub async fn create_streaming_writer<'b, T: Serialize>(
        &self,
        table: impl Into<BigQueryTableRef>,
    ) -> BigQueryResult<(
        BigQueryStreamingWriter<T>,
        BoxStream<'b, BigQueryResult<BigQueryWriteResponse>>,
    )> {
        self.create_streaming_writer_with_options(table, BigQueryStreamingWriteOptions::new())
            .await
    }

    /// Opens a streaming writer on `table` with `options`. A committed or pending mode
    /// creates a new write stream.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for options that cannot be sent, such as a
    /// `max_request_bytes` over 19,000,000; otherwise the failure to open the stream.
    pub async fn create_streaming_writer_with_options<'b, T: Serialize>(
        &self,
        table: impl Into<BigQueryTableRef>,
        options: BigQueryStreamingWriteOptions,
    ) -> BigQueryResult<(
        BigQueryStreamingWriter<T>,
        BoxStream<'b, BigQueryResult<BigQueryWriteResponse>>,
    )> {
        let (core, responses) = WriterCore::open(self, table.into(), options, false).await?;
        Ok((
            BigQueryStreamingWriter {
                core,
                _row: PhantomData,
            },
            responses,
        ))
    }

    /// Commits finalized pending streams of one table together: all of their rows become
    /// visible at the returned commit time, or none do.
    ///
    /// # Errors
    /// [`BigQueryError::InvalidParametersError`] for no streams or streams of more than one
    /// table; [`BigQueryError::WriteStreamError`] when BigQuery reports a stream it could not
    /// commit.
    pub async fn commit_write_streams(
        &self,
        streams: Vec<BigQueryFinalizedStream>,
    ) -> BigQueryResult<BigQueryInstant> {
        let Some(first) = streams.first() else {
            return Err(BigQueryError::invalid_parameters(
                "streams",
                "there is no stream to commit",
            ));
        };
        let table = first.table.clone();
        if let Some(other) = streams.iter().find(|s| s.table != table) {
            return Err(BigQueryError::invalid_parameters(
                "streams",
                format!(
                    "BigQuery commits streams of one table together, got {table} and {}",
                    other.table
                ),
            ));
        }
        let table_path = table.table_path(&self.options().google_project_id);
        let span = debug_span!("BigQuery commit write streams", "/bigquery/table" = %table);
        self.batch_commit(
            &span,
            &table_path,
            streams.into_iter().map(|s| s.name.to_string()).collect(),
        )
        .await
    }
}
