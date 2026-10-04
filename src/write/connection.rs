//! The background task that owns a writer's `AppendRows` connection.
//!
//! The writer handle encodes rows and seals batches into [`Shared`]; this task sends them in
//! order, matches each response to its request (BigQuery answers one connection in request
//! order), resends what the delivery guarantee allows, and reports one outcome per batch in
//! batch order.
//!
//! In committed and pending mode a batch's offset is the number of rows written before it, so a
//! batch that is not written moves every later one down. The task stops sending after such a
//! batch, takes back the later ones as BigQuery answers them (`OFFSET_OUT_OF_RANGE` or
//! `ABORTED`), and sends them again at their new offsets once nothing is in flight.

use crate::errors::{
    BigQueryError, BigQueryErrorPublicGenericDetails, BigQueryRowError, BigQueryRowErrors,
    BigQuerySchemaMismatchError, BigQuerySystemError, BigQueryWriteStreamError,
};
use crate::write::batch::{append_request, Batch, Batcher, RequestTarget};
use crate::write::descriptor::WritePlan;
use crate::write::writer::{batch_commit, finalize_write_stream, get_write_stream};
use crate::{
    BigQueryDb, BigQueryResult, BigQueryTableRef, BigQueryTableSchema, BigQueryWriteMode,
    BigQueryWriteResponse, BigQueryWriteSummary,
};
use crate::{BigQueryInstant, BigQueryWriteStreamName};
use futures::channel::mpsc as stream_channel;
use gcloud_sdk::google::cloud::bigquery::storage::v1::append_rows_response::Response;
use gcloud_sdk::google::cloud::bigquery::storage::v1::row_error::RowErrorCode;
use gcloud_sdk::google::cloud::bigquery::storage::v1::storage_error::StorageErrorCode;
use gcloud_sdk::google::cloud::bigquery::storage::v1::{
    AppendRowsRequest, AppendRowsResponse, RowError, StorageError,
};
use gcloud_sdk::prost::Message;
use gcloud_sdk::tonic::{Code, Status};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, warn, Span};

/// What the writer handle asks of its task.
pub(crate) enum Command {
    /// A batch was sealed or opened.
    Wake,
    /// Answer once every batch sealed so far has an outcome.
    Flush(oneshot::Sender<BigQueryResult<()>>),
    /// Reconnect once the requests in flight are answered.
    Reconnect,
    /// Close the connection and end the stream as `FinishKind` says, once idle.
    Finish(FinishKind, oneshot::Sender<BigQueryResult<Finished>>),
}

/// How a finished writer leaves its stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FinishKind {
    /// Close the default stream, finalize a committed one, finalize and commit a pending one.
    Close,
    /// Finalize a pending stream and leave the commit to the caller.
    Finalize,
}

/// What a finished writer reports.
#[derive(Debug)]
pub(crate) struct Finished {
    pub(crate) summary: BigQueryWriteSummary,
    /// The first batch that failed, if any.
    pub(crate) first_error: Option<BigQueryError>,
    /// The row count `FinalizeWriteStream` reported.
    pub(crate) finalized_rows: Option<i64>,
}

/// The state the writer handle and its task share.
pub(crate) struct State {
    pub(crate) batcher: Batcher,
    /// Sealed batches the task has not taken yet.
    pub(crate) sealed: VecDeque<Batch>,
    /// Sealed batches without an outcome yet, and their bytes.
    pub(crate) inflight_requests: usize,
    pub(crate) inflight_bytes: usize,
    /// A schema BigQuery reported for the next batches.
    pub(crate) pending_schema: Option<BigQueryTableSchema>,
    /// Set once the writer has failed for good.
    pub(crate) failed: Option<BigQueryError>,
}

pub(crate) struct Shared {
    state: Mutex<State>,
    /// Notified whenever a batch gets its outcome or the writer fails.
    pub(crate) capacity: Notify,
    max_requests: usize,
    max_bytes: usize,
}

impl Shared {
    pub(crate) fn new(batcher: Batcher, max_requests: usize, max_bytes: usize) -> Self {
        Shared {
            state: Mutex::new(State {
                batcher,
                sealed: VecDeque::new(),
                inflight_requests: 0,
                inflight_bytes: 0,
                pending_schema: None,
                failed: None,
            }),
            capacity: Notify::new(),
            max_requests,
            max_bytes,
        }
    }

    /// The state stays consistent under a panic elsewhere, since every update is a few field
    /// assignments, so a poisoned lock is taken as it is.
    pub(crate) fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether sealing the open batch keeps the unacknowledged requests and bytes within the
    /// limits. A batch larger than the byte limit alone still goes when nothing is in flight.
    pub(crate) fn can_seal(&self, state: &State) -> bool {
        let bytes = state.batcher.open_bytes().unwrap_or(0);
        state.inflight_requests < self.max_requests
            && (state.inflight_requests == 0 || state.inflight_bytes + bytes <= self.max_bytes)
    }

    /// Seals the open batch into the queue for the task.
    pub(crate) fn seal_locked(&self, state: &mut State) {
        if let Some(batch) = state.batcher.seal() {
            state.inflight_requests += 1;
            state.inflight_bytes += batch.bytes;
            state.sealed.push_back(batch);
        }
    }

    fn release(&self, batch: &Batch) {
        {
            let mut state = self.lock();
            state.inflight_requests = state.inflight_requests.saturating_sub(1);
            state.inflight_bytes = state.inflight_bytes.saturating_sub(batch.bytes);
        }
        self.capacity.notify_waiters();
    }
}

/// A copy of `err`, since one failure is reported to every batch and call it ends.
pub(crate) fn clone_error(err: &BigQueryError) -> BigQueryError {
    match err {
        BigQueryError::SystemError(e) => BigQueryError::SystemError(e.clone()),
        BigQueryError::DatabaseError(e) => BigQueryError::DatabaseError(e.clone()),
        BigQueryError::DataConflictError(e) => BigQueryError::DataConflictError(e.clone()),
        BigQueryError::DataNotFoundError(e) => BigQueryError::DataNotFoundError(e.clone()),
        BigQueryError::InvalidParametersError(e) => {
            BigQueryError::InvalidParametersError(e.clone())
        }
        BigQueryError::SerializeError(e) => BigQueryError::SerializeError(e.clone()),
        BigQueryError::DeserializeError(e) => BigQueryError::DeserializeError(e.clone()),
        BigQueryError::RowErrors(e) => BigQueryError::RowErrors(e.clone()),
        BigQueryError::SchemaMismatchError(e) => BigQueryError::SchemaMismatchError(e.clone()),
        BigQueryError::WriteStreamError(e) => BigQueryError::WriteStreamError(e.clone()),
        BigQueryError::JobError(e) => BigQueryError::JobError(e.clone()),
        BigQueryError::SchemaChangeRefused(e) => BigQueryError::SchemaChangeRefused(e.clone()),
    }
}

/// The error for a writer whose task is gone, which only happens if it panicked.
pub(crate) fn task_ended() -> BigQueryError {
    BigQueryError::SystemError(BigQuerySystemError::new(
        BigQueryErrorPublicGenericDetails::new("WRITER_TASK_ENDED".into()),
        "the streaming writer's background task ended unexpectedly".into(),
    ))
}

/// The `StorageError` BigQuery attaches to an in-band append error.
fn storage_error(status: &gcloud_sdk::google::rpc::Status) -> Option<StorageError> {
    status
        .details
        .iter()
        .find(|any| {
            any.type_url
                .ends_with("google.cloud.bigquery.storage.v1.StorageError")
        })
        .and_then(|any| StorageError::decode(any.value.as_slice()).ok())
}

struct Sent {
    batch: Batch,
    offset: Option<i64>,
}

struct Connection {
    requests: stream_channel::UnboundedSender<AppendRowsRequest>,
    responses: mpsc::UnboundedReceiver<Result<AppendRowsResponse, Status>>,
    reader: JoinHandle<()>,
    /// The plan whose descriptor this connection was last sent.
    plan: Option<Arc<WritePlan>>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

enum Event {
    Command(Option<Command>),
    Response(Result<Option<AppendRowsResponse>, Status>),
    Timer,
}

/// The settings a task runs with.
pub(crate) struct TaskSettings {
    pub(crate) db: BigQueryDb,
    pub(crate) span: Span,
    pub(crate) mode: BigQueryWriteMode,
    pub(crate) target: RequestTarget,
    pub(crate) table: BigQueryTableRef,
    pub(crate) table_path: String,
    pub(crate) batch_delay: Duration,
}

pub(crate) struct ConnectionTask {
    settings: TaskSettings,
    max_retries: usize,
    shared: Arc<Shared>,
    commands: mpsc::UnboundedReceiver<Command>,
    responses: stream_channel::UnboundedSender<BigQueryResult<BigQueryWriteResponse>>,
    queue: VecDeque<Batch>,
    /// Batches taken back while draining, in send order.
    requeued: Vec<Batch>,
    inflight: VecDeque<Sent>,
    conn: Option<Connection>,
    /// No new request is sent until everything in flight is answered.
    draining: bool,
    reconnect_after_drain: bool,
    next_offset: i64,
    /// Rows written on the stream, the offset of the next batch once nothing is in flight.
    written_offset: i64,
    reconnects: usize,
    outcomes: BTreeMap<u64, BigQueryResult<BigQueryWriteResponse>>,
    next_emit: u64,
    rows_written: u64,
    rows_failed: u64,
    batches: u64,
    /// `AppendRows` requests sent, resends included, and their encoded bytes.
    appends: u64,
    bytes_sent: u64,
    /// Requests that sent a batch again.
    retries: u64,
    first_error: Option<BigQueryError>,
    fatal: Option<BigQueryError>,
    flush_waiters: Vec<oneshot::Sender<BigQueryResult<()>>>,
    finish: Option<(FinishKind, oneshot::Sender<BigQueryResult<Finished>>)>,
}

impl ConnectionTask {
    pub(crate) fn new(
        settings: TaskSettings,
        shared: Arc<Shared>,
        commands: mpsc::UnboundedReceiver<Command>,
        responses: stream_channel::UnboundedSender<BigQueryResult<BigQueryWriteResponse>>,
    ) -> Self {
        let max_retries = settings.db.options().max_retries;
        ConnectionTask {
            settings,
            max_retries,
            shared,
            commands,
            responses,
            queue: VecDeque::new(),
            requeued: Vec::new(),
            inflight: VecDeque::new(),
            conn: None,
            draining: false,
            reconnect_after_drain: false,
            next_offset: 0,
            written_offset: 0,
            reconnects: 0,
            outcomes: BTreeMap::new(),
            next_emit: 0,
            rows_written: 0,
            rows_failed: 0,
            batches: 0,
            appends: 0,
            bytes_sent: 0,
            retries: 0,
            first_error: None,
            fatal: None,
            flush_waiters: Vec::new(),
            finish: None,
        }
    }

    fn uses_offsets(&self) -> bool {
        self.settings.mode != BigQueryWriteMode::Default
    }

    pub(crate) async fn run(mut self) {
        loop {
            self.pull_sealed();
            self.send_ready().await;
            self.answer_flushes();
            if self.finish.is_some() && (self.fatal.is_some() || self.is_idle()) {
                break;
            }
            let deadline = self.timer_deadline();
            let event = tokio::select! {
                command = self.commands.recv() => Event::Command(command),
                response = next_response(&mut self.conn),
                    if self.conn.is_some() && !self.inflight.is_empty() => Event::Response(response),
                () = sleep_until(deadline), if deadline.is_some() => Event::Timer,
            };
            match event {
                // The writer was dropped without finish(); the warning is the writer's.
                Event::Command(None) => return,
                Event::Command(Some(Command::Wake)) => {}
                Event::Command(Some(Command::Flush(waiter))) => self.flush_waiters.push(waiter),
                Event::Command(Some(Command::Reconnect)) => self.request_reconnect(),
                Event::Command(Some(Command::Finish(kind, waiter))) => {
                    self.finish = Some((kind, waiter));
                }
                Event::Response(Ok(Some(response))) => self.on_append_response(response).await,
                Event::Response(Ok(None)) => {
                    self.on_stream_error(Status::unavailable(
                        "the AppendRows stream ended with requests unanswered",
                    ))
                    .await;
                }
                Event::Response(Err(status)) => self.on_stream_error(status).await,
                Event::Timer => self.on_timer(),
            }
        }
        self.complete().await;
    }

    fn pull_sealed(&mut self) {
        let sealed: Vec<Batch> = self.shared.lock().sealed.drain(..).collect();
        for batch in sealed {
            self.batches += 1;
            match &self.fatal {
                Some(err) => {
                    let err = clone_error(err);
                    self.fail_batch(batch, err);
                }
                None => self.queue.push_back(batch),
            }
        }
    }

    fn is_idle(&self) -> bool {
        self.queue.is_empty() && self.requeued.is_empty() && self.inflight.is_empty() && {
            let state = self.shared.lock();
            state.sealed.is_empty() && state.batcher.open_bytes().is_none()
        }
    }

    fn answer_flushes(&mut self) {
        if self.flush_waiters.is_empty() || (self.fatal.is_none() && !self.is_idle()) {
            return;
        }
        for waiter in self.flush_waiters.drain(..) {
            let answer = match &self.fatal {
                Some(err) => Err(clone_error(err)),
                None => Ok(()),
            };
            let _ = waiter.send(answer);
        }
    }

    fn timer_deadline(&self) -> Option<Instant> {
        if self.fatal.is_some() {
            return None;
        }
        let state = self.shared.lock();
        let since = state.batcher.open_since()?;
        self.shared
            .can_seal(&state)
            .then(|| since + self.settings.batch_delay)
    }

    fn on_timer(&mut self) {
        let mut state = self.shared.lock();
        let due = state
            .batcher
            .open_since()
            .is_some_and(|since| since + self.settings.batch_delay <= Instant::now());
        if due && self.shared.can_seal(&state) {
            self.shared.seal_locked(&mut state);
        }
    }

    fn request_reconnect(&mut self) {
        if self.inflight.is_empty() {
            self.conn = None;
        } else {
            self.draining = true;
            self.reconnect_after_drain = true;
        }
    }

    async fn send_ready(&mut self) {
        while self.fatal.is_none() && !self.draining {
            let Some(mut batch) = self.queue.pop_front() else {
                break;
            };
            let offset = self.uses_offsets().then_some(self.next_offset);
            let needs_schema = !self.conn.as_ref().is_some_and(|conn| {
                conn.plan
                    .as_ref()
                    .is_some_and(|plan| Arc::ptr_eq(plan, &batch.plan))
            });
            let request = append_request(
                &self.settings.target,
                offset,
                needs_schema.then_some(&*batch.plan),
                batch.rows.clone(),
            );
            batch.attempts += 1;
            let bytes = request.encoded_len() as u64;
            let sent = match self.conn.as_mut() {
                Some(conn) => conn.requests.unbounded_send(request).is_ok(),
                None => {
                    self.conn = Some(self.open(request));
                    true
                }
            };
            if !sent {
                self.queue.push_front(batch);
                self.on_stream_error(Status::unavailable("the AppendRows request stream closed"))
                    .await;
                continue;
            }
            self.appends += 1;
            self.bytes_sent += bytes;
            if batch.attempts > 1 {
                self.retries += 1;
            }
            if let Some(conn) = self.conn.as_mut() {
                conn.plan = Some(batch.plan.clone());
            }
            self.next_offset += batch.row_count() as i64;
            self.inflight.push_back(Sent { batch, offset });
        }
    }

    /// Opens a connection whose first request is `first`. The request goes into the stream
    /// before the call, since the server may wait for it before it answers with headers, and
    /// a reader task waits for the headers and the responses, so this task never blocks on
    /// them.
    fn open(&self, first: AppendRowsRequest) -> Connection {
        let (requests, outgoing) = stream_channel::unbounded();
        let _ = requests.unbounded_send(first);
        debug!(parent: &self.settings.span, "Opening an AppendRows connection.");
        let (forward, responses) = mpsc::unbounded_channel();
        let mut client = self.settings.db.write_client();
        let reader = tokio::spawn(async move {
            let mut stream = match client.append_rows(outgoing).await {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    let _ = forward.send(Err(status));
                    return;
                }
            };
            loop {
                match stream.message().await {
                    Ok(Some(response)) => {
                        if forward.send(Ok(response)).is_err() {
                            return;
                        }
                    }
                    Ok(None) => return,
                    Err(status) => {
                        let _ = forward.send(Err(status));
                        return;
                    }
                }
            }
        });
        Connection {
            requests,
            responses,
            reader,
            plan: None,
        }
    }

    async fn on_append_response(&mut self, response: AppendRowsResponse) {
        let Some(sent) = self.inflight.pop_front() else {
            warn!(parent: &self.settings.span, "An AppendRows response came with no request in flight.");
            return;
        };
        if let Some(schema) = &response.updated_schema {
            match BigQueryTableSchema::try_from(schema) {
                Ok(schema) => self.shared.lock().pending_schema = Some(schema),
                Err(err) => warn!(
                    parent: &self.settings.span,
                    %err,
                    "Ignoring an updated schema the writer cannot use."
                ),
            }
        }
        match response.response {
            Some(Response::Error(status)) => {
                self.on_in_band(sent, status, response.row_errors).await;
            }
            Some(Response::AppendResult(result)) => self.acknowledge(sent, result.offset),
            None => self.acknowledge(sent, None),
        }
        self.after_resolution();
    }

    async fn on_in_band(
        &mut self,
        sent: Sent,
        status: gcloud_sdk::google::rpc::Status,
        row_errors: Vec<RowError>,
    ) {
        let code = Code::from(status.code);
        let storage = storage_error(&status).and_then(|e| StorageErrorCode::try_from(e.code).ok());
        if !row_errors.is_empty() {
            let batch = &sent.batch;
            let errors = row_errors
                .iter()
                .map(|e| {
                    let code = RowErrorCode::try_from(e.code)
                        .map(|c| c.as_str_name().to_string())
                        .unwrap_or_else(|_| e.code.to_string());
                    BigQueryRowError::new(
                        batch.first_row + u64::try_from(e.index).unwrap_or_default(),
                        code,
                        e.message.clone(),
                    )
                })
                .collect();
            let err = BigQueryError::RowErrors(BigQueryRowErrors::new(
                BigQueryErrorPublicGenericDetails::new("ROW_ERRORS".into()),
                batch.index,
                batch.first_row,
                batch.row_count(),
                errors,
            ));
            self.batch_not_written(sent, err);
            return;
        }
        use StorageErrorCode as S;
        let schema_mismatch = storage == Some(S::SchemaMismatchExtraFields)
            || status
                .message
                .contains("Input schema has more fields than BigQuery schema");
        match (storage, code) {
            (Some(S::OffsetAlreadyExists), _) | (None, Code::AlreadyExists)
                if self.uses_offsets() =>
            {
                let offset = sent.offset;
                self.acknowledge(sent, offset);
            }
            (Some(S::OffsetOutOfRange), _) | (_, Code::OutOfRange | Code::Aborted) => {
                self.requeue(sent, Status::new(code, status.message));
            }
            _ if schema_mismatch => {
                let err = BigQueryError::SchemaMismatchError(BigQuerySchemaMismatchError::new(
                    BigQueryErrorPublicGenericDetails::new(
                        S::SchemaMismatchExtraFields.as_str_name().into(),
                    ),
                    self.settings.table.clone(),
                    status.message,
                ));
                self.batch_not_written(sent, err);
                self.refresh_schema().await;
            }
            (_, Code::Internal) => self.requeue(sent, Status::new(code, status.message)),
            (
                Some(
                    storage @ (S::StreamFinalized
                    | S::StreamNotFound
                    | S::StreamAlreadyCommitted
                    | S::InvalidStreamState
                    | S::InvalidStreamType),
                ),
                _,
            ) => {
                self.inflight.push_front(sent);
                self.fail_writer(BigQueryError::WriteStreamError(
                    BigQueryWriteStreamError::new(
                        BigQueryErrorPublicGenericDetails::new(storage.as_str_name().into()),
                        self.settings.target.write_stream.clone(),
                        status.message,
                    ),
                ));
            }
            (Some(S::TableNotFound), _) => {
                self.inflight.push_front(sent);
                self.fail_writer(BigQueryError::from(Status::not_found(status.message)));
            }
            _ => {
                let err = BigQueryError::from(Status::new(code, status.message));
                self.batch_not_written(sent, err);
            }
        }
    }

    /// Reads the stream's schema again after BigQuery rejected the writer's, so the writer
    /// encodes the next rows against the table as it is now.
    async fn refresh_schema(&mut self) {
        let fetched = get_write_stream(
            &self.settings.db,
            &self.settings.span,
            &self.settings.target.write_stream,
        )
        .await
        .and_then(|stream| crate::write::writer::stream_schema(&stream));
        match fetched {
            Ok(schema) => self.shared.lock().pending_schema = Some(schema),
            Err(err) => warn!(
                parent: &self.settings.span,
                %err,
                "Failed to read the write stream's schema after a schema mismatch."
            ),
        }
    }

    fn acknowledge(&mut self, sent: Sent, result_offset: Option<i64>) {
        self.reconnects = 0;
        let batch = sent.batch;
        let rows = batch.row_count();
        self.rows_written += rows;
        if self.uses_offsets() {
            self.written_offset += rows as i64;
        }
        let response = BigQueryWriteResponse {
            batch_index: batch.index,
            first_row: batch.first_row,
            row_count: rows,
            offset: sent.offset.or(result_offset),
        };
        self.resolve(batch, Ok(response));
    }

    fn batch_not_written(&mut self, sent: Sent, err: BigQueryError) {
        self.fail_batch(sent.batch, err);
        if self.uses_offsets() {
            self.draining = true;
        }
    }

    fn requeue(&mut self, sent: Sent, status: Status) {
        if sent.batch.attempts > self.max_retries + 1 {
            let err = BigQueryError::from(status);
            self.batch_not_written(sent, err);
            return;
        }
        debug!(
            parent: &self.settings.span,
            batch = sent.batch.index,
            %status,
            "Sending a batch again."
        );
        self.requeued.push(sent.batch);
        self.draining = true;
    }

    fn after_resolution(&mut self) {
        if !self.draining || !self.inflight.is_empty() {
            return;
        }
        for batch in self.requeued.drain(..).rev() {
            self.queue.push_front(batch);
        }
        self.next_offset = self.written_offset;
        self.draining = false;
        if self.reconnect_after_drain {
            self.reconnect_after_drain = false;
            self.conn = None;
        }
    }

    async fn on_stream_error(&mut self, status: Status) {
        let err = if status
            .message()
            .contains("The proto field mismatched with BigQuery field")
        {
            BigQueryError::SchemaMismatchError(BigQuerySchemaMismatchError::new(
                BigQueryErrorPublicGenericDetails::new(format!("{:?}", status.code())),
                self.settings.table.clone(),
                status.message().to_string(),
            ))
        } else {
            BigQueryError::from(status)
        };
        self.conn = None;
        let unanswered: Vec<Batch> = self
            .requeued
            .drain(..)
            .chain(self.inflight.drain(..).map(|sent| sent.batch))
            .collect();
        for batch in unanswered.into_iter().rev() {
            self.queue.push_front(batch);
        }
        self.draining = false;
        self.reconnect_after_drain = false;
        self.next_offset = self.written_offset;
        if err.retry_possible() && self.reconnects < self.max_retries {
            let delay = crate::db::retry_delay(self.reconnects);
            self.reconnects += 1;
            warn!(
                parent: &self.settings.span,
                %err,
                attempt = self.reconnects,
                delay = delay.as_millis(),
                "The AppendRows connection failed; reconnecting and sending the unacknowledged \
                 batches again."
            );
            tokio::time::sleep(delay).await;
        } else {
            self.fail_writer(err);
        }
    }

    /// Ends the writer: every batch without an outcome fails with `err`, and so does every
    /// later call on the writer.
    fn fail_writer(&mut self, err: BigQueryError) {
        warn!(parent: &self.settings.span, %err, "The streaming writer failed.");
        self.conn = None;
        let mut pending: Vec<Batch> = std::mem::take(&mut self.requeued);
        pending.extend(self.inflight.drain(..).map(|sent| sent.batch));
        pending.extend(self.queue.drain(..));
        {
            let mut state = self.shared.lock();
            state.failed = Some(clone_error(&err));
            self.shared.seal_locked(&mut state);
            let sealed: Vec<Batch> = state.sealed.drain(..).collect();
            drop(state);
            self.batches += sealed.len() as u64;
            pending.extend(sealed);
        }
        for batch in pending {
            self.fail_batch(batch, clone_error(&err));
        }
        self.fatal = Some(err);
        self.shared.capacity.notify_waiters();
    }

    fn fail_batch(&mut self, batch: Batch, err: BigQueryError) {
        self.rows_failed += batch.row_count();
        if self.first_error.is_none() {
            self.first_error = Some(clone_error(&err));
        }
        self.resolve(batch, Err(err));
    }

    fn resolve(&mut self, batch: Batch, outcome: BigQueryResult<BigQueryWriteResponse>) {
        self.shared.release(&batch);
        self.outcomes.insert(batch.index, outcome);
        while let Some(outcome) = self.outcomes.remove(&self.next_emit) {
            let _ = self.responses.unbounded_send(outcome);
            self.next_emit += 1;
        }
    }

    fn summary(&self, commit_time: Option<BigQueryInstant>) -> BigQueryWriteSummary {
        BigQueryWriteSummary {
            rows_written: self.rows_written,
            rows_failed: self.rows_failed,
            batches: self.batches,
            bytes_sent: self.bytes_sent,
            stream: self.uses_offsets().then(|| {
                BigQueryWriteStreamName::reported(self.settings.target.write_stream.clone())
            }),
            commit_time,
        }
    }

    async fn complete(mut self) {
        self.conn = None;
        let Some((kind, waiter)) = self.finish.take() else {
            return;
        };
        let result = match &self.fatal {
            Some(err) => Err(clone_error(err)),
            None => self.close_stream(kind).await,
        };
        let _ = waiter.send(result);
    }

    async fn close_stream(&mut self, kind: FinishKind) -> BigQueryResult<Finished> {
        let stream = self.settings.target.write_stream.clone();
        let first_error = self.first_error.as_ref().map(clone_error);
        match self.settings.mode {
            BigQueryWriteMode::Default => Ok(Finished {
                summary: self.summary(None),
                first_error,
                finalized_rows: None,
            }),
            BigQueryWriteMode::Committed => {
                let rows =
                    finalize_write_stream(&self.settings.db, &self.settings.span, &stream).await?;
                Ok(Finished {
                    summary: self.summary(None),
                    first_error,
                    finalized_rows: Some(rows),
                })
            }
            BigQueryWriteMode::Pending => {
                if self.rows_failed > 0 {
                    return Err(BigQueryError::WriteStreamError(
                        BigQueryWriteStreamError::new(
                            BigQueryErrorPublicGenericDetails::new("NOT_COMMITTED".into()),
                            stream,
                            format!(
                                "{} rows were in failed batches, so the pending stream was not \
                             committed and none of its rows are visible; the first failure: {}",
                                self.rows_failed,
                                first_error
                                    .as_ref()
                                    .map(ToString::to_string)
                                    .unwrap_or_default()
                            ),
                        ),
                    ));
                }
                let rows =
                    finalize_write_stream(&self.settings.db, &self.settings.span, &stream).await?;
                let commit_time = match kind {
                    FinishKind::Finalize => None,
                    FinishKind::Close => Some(
                        batch_commit(
                            &self.settings.db,
                            &self.settings.span,
                            &self.settings.table_path,
                            vec![stream],
                        )
                        .await?,
                    ),
                };
                Ok(Finished {
                    summary: self.summary(commit_time),
                    first_error,
                    finalized_rows: Some(rows),
                })
            }
        }
    }
}

/// Records what the task sent on the writer's span when it ends: after `finish()` or
/// `finalize()`, when the writer is dropped, or when the task fails.
impl Drop for ConnectionTask {
    fn drop(&mut self) {
        let span = &self.settings.span;
        span.record("/bigquery/rows_appended", self.rows_written);
        span.record("/bigquery/bytes_sent", self.bytes_sent);
        span.record("/bigquery/appends", self.appends);
        span.record("/bigquery/retries", self.retries);
    }
}

async fn next_response(
    conn: &mut Option<Connection>,
) -> Result<Option<AppendRowsResponse>, Status> {
    match conn {
        Some(conn) => conn.responses.recv().await.transpose(),
        None => std::future::pending().await,
    }
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}
