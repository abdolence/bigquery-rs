//! Read streams: one task per stream, resumed at its row offset, merged into one stream.

use crate::errors::BigQueryError;
use crate::read::ipc::ArrowIpcDecoder;
use crate::read::session::OpenedSession;
use crate::{BigQueryDb, BigQueryResult};
use arrow_array::RecordBatch;
use futures::stream::BoxStream;
use futures::StreamExt;
use gcloud_sdk::google::cloud::bigquery::storage::v1::read_rows_response::Rows;
use gcloud_sdk::google::cloud::bigquery::storage::v1::{ReadRowsRequest, ReadRowsResponse};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::{warn, Span};

/// How BigQuery fails a stream holding an INTERVAL whose time part does not fit Arrow's `i64`
/// nanoseconds. Every resume would fail the same way.
const INTERVAL_OVERFLOW: &str = "Out of range conversion for microseconds value";

/// What a stream task sends: one item per record batch, or the failure that ended the stream.
enum StreamMessage<M> {
    Batch(M),
    Failed(BigQueryError),
}

/// What every stream of one read has received so far.
#[derive(Default)]
struct ReadTotals {
    rows: AtomicI64,
    /// Summed only over the responses that report their size.
    bytes: AtomicI64,
    bytes_reported: AtomicBool,
    /// The highest throttling any response reported.
    throttle_percent: AtomicI32,
    throttle_reported: AtomicBool,
}

impl ReadTotals {
    fn add(&self, response: &ReadRowsResponse) {
        self.rows.fetch_add(response.row_count, Ordering::Relaxed);
        if let Some(bytes) = response.uncompressed_byte_size {
            self.bytes.fetch_add(bytes, Ordering::Relaxed);
            self.bytes_reported.store(true, Ordering::Relaxed);
        }
        if let Some(throttle) = &response.throttle_state {
            self.throttle_percent
                .fetch_max(throttle.throttle_percent, Ordering::Relaxed);
            self.throttle_reported.store(true, Ordering::Relaxed);
        }
    }

    fn record(&self, span: &Span) {
        span.record("/bigquery/rows_read", self.rows.load(Ordering::Relaxed));
        if self.bytes_reported.load(Ordering::Relaxed) {
            span.record("/bigquery/bytes_read", self.bytes.load(Ordering::Relaxed));
        }
        if self.throttle_reported.load(Ordering::Relaxed) {
            span.record(
                "/bigquery/throttle_percent",
                self.throttle_percent.load(Ordering::Relaxed),
            );
        }
    }
}

/// The read streams of one session, running. Dropping it cancels every stream task and
/// records what they received on the read's span, so a read that ends, fails or is abandoned
/// reports its totals the same way.
pub(crate) struct RunningStreams<M> {
    rx: mpsc::Receiver<StreamMessage<M>>,
    tasks: JoinSet<()>,
    span: Span,
    totals: Arc<ReadTotals>,
}

impl BigQueryDb {
    /// Starts one task per stream of `session`. Each task decodes its batches with `on_batch`,
    /// given the batch and the row offset of its first row in the stream. At most two items per
    /// stream wait for the caller, so a slow consumer slows the streams down rather than growing
    /// memory.
    pub(crate) fn start_read_streams<M, F>(
        &self,
        session: OpenedSession,
        span: &Span,
        on_batch: F,
    ) -> RunningStreams<M>
    where
        M: Send + 'static,
        F: Fn(RecordBatch, u64) -> M + Clone + Send + 'static,
    {
        let (tx, rx) = mpsc::channel((2 * session.streams.len()).max(1));
        let schema: Arc<[u8]> = session.schema.into();
        let totals = Arc::new(ReadTotals::default());
        let mut tasks = JoinSet::new();
        for stream in session.streams {
            let stream_task = StreamTask {
                db: self.clone(),
                stream,
                schema: schema.clone(),
                span: span.clone(),
                totals: totals.clone(),
            };
            let (tx, on_batch) = (tx.clone(), on_batch.clone());
            tasks.spawn(async move {
                if let Err(err) = stream_task.pump(&tx, on_batch).await {
                    let _ = tx.send(StreamMessage::Failed(err)).await;
                }
            });
        }
        RunningStreams {
            rx,
            tasks,
            span: span.clone(),
            totals,
        }
    }
}

impl RunningStreams<RecordBatch> {
    /// The batches of every stream as they arrive. A stream that fails ends the whole stream
    /// after its error, since a scan that lost a stream is incomplete.
    pub(crate) fn into_batches<'b>(self) -> BoxStream<'b, BigQueryResult<RecordBatch>> {
        futures::stream::unfold(Some(self), |state| async move {
            let mut running = state?;
            match running.rx.recv().await? {
                StreamMessage::Batch(batch) => Some((Ok(batch), Some(running))),
                StreamMessage::Failed(err) => Some((Err(err), None)),
            }
        })
        .boxed()
    }
}

impl<T: Send + 'static> RunningStreams<Vec<BigQueryResult<T>>> {
    /// The rows of every stream as they arrive. A row that failed to decode is one `Err` and
    /// the stream goes on; a stream that fails ends the whole stream after its error.
    pub(crate) fn into_rows<'b>(self) -> BoxStream<'b, BigQueryResult<T>> {
        let state = (self, Vec::new().into_iter());
        futures::stream::unfold(Some(state), |state| async move {
            let (mut running, mut pending) = state?;
            loop {
                if let Some(row) = pending.next() {
                    return Some((row, Some((running, pending))));
                }
                match running.rx.recv().await? {
                    StreamMessage::Batch(rows) => pending = rows.into_iter(),
                    StreamMessage::Failed(err) => return Some((Err(err), None)),
                }
            }
        })
        .boxed()
    }
}

impl<M> Drop for RunningStreams<M> {
    fn drop(&mut self) {
        self.tasks.abort_all();
        self.totals.record(&self.span);
    }
}

struct StreamTask {
    db: BigQueryDb,
    stream: String,
    schema: Arc<[u8]>,
    span: Span,
    totals: Arc<ReadTotals>,
}

impl StreamTask {
    /// Reads the stream to its end, sending one item per batch, and resumes at the offset
    /// after a retryable failure. Consecutive failures are capped by `max_retries`; a batch
    /// that arrives resets the count. Returns early, without error, once the caller is gone.
    async fn pump<M, F>(
        &self,
        tx: &mpsc::Sender<StreamMessage<M>>,
        on_batch: F,
    ) -> BigQueryResult<()>
    where
        F: Fn(RecordBatch, u64) -> M,
    {
        let mut decoder = ArrowIpcDecoder::new(&self.schema)?;
        let max_retries = self.db.options().max_retries;
        // The rows received so far: the offset a resumed `ReadRows` starts from.
        let mut offset: i64 = 0;
        let mut failures = 0usize;
        loop {
            let request = ReadRowsRequest {
                read_stream: self.stream.clone(),
                offset,
                ..Default::default()
            };
            let status = match self.db.read_client().read_rows(request).await {
                Ok(response) => {
                    let mut responses = response.into_inner();
                    loop {
                        match responses.message().await {
                            Ok(Some(response)) => {
                                self.totals.add(&response);
                                let rows = match response.rows {
                                    Some(Rows::ArrowRecordBatch(rows)) => rows,
                                    Some(Rows::AvroRows(_)) => {
                                        return Err(BigQueryError::unexpected_response(
                                            "The read stream sent Avro rows to an Arrow session",
                                        ))
                                    }
                                    None => continue,
                                };
                                let batch = decoder.decode(&rows.serialized_record_batch)?;
                                let first_row = u64::try_from(offset).unwrap_or_default();
                                offset += response.row_count;
                                failures = 0;
                                let item = StreamMessage::Batch(on_batch(batch, first_row));
                                if tx.send(item).await.is_err() {
                                    return Ok(());
                                }
                            }
                            Ok(None) => return Ok(()),
                            Err(status) => break status,
                        }
                    }
                }
                Err(status) => status,
            };
            let err = if status.message().contains(INTERVAL_OVERFLOW) {
                BigQueryError::database(
                    status.code(),
                    format!(
                        "{status}. An INTERVAL with a time part beyond about 2,562,047 hours \
                         cannot be read through the Storage Read API; select CAST(column AS \
                         STRING) in a query instead"
                    ),
                    false,
                )
            } else {
                BigQueryError::from(status)
            };
            if !err.retry_possible() || failures >= max_retries {
                return Err(err);
            }
            let delay = crate::db::retry_delay(failures);
            failures += 1;
            self.span.in_scope(|| {
                warn!(
                    %err,
                    stream = self.stream,
                    offset,
                    current_retry = failures,
                    max_retries,
                    delay = delay.as_millis(),
                    "Failed to read rows. Resuming the stream at its offset.",
                );
            });
            tokio::time::sleep(delay).await;
        }
    }
}
