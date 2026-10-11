//! Storage Read sessions: a session pins one snapshot of a table, plans its scan into file
//! tasks once, and splits the tasks into streams; `ReadRows` reads one stream's tasks in order.
//!
//! A stream's rows come in a fixed order: its tasks in the order the session assigned them, and
//! each task's rows in file order, since the reader takes one data file at a time (at a higher
//! data file concurrency iceberg's reader interleaves files). `ReadRows(offset)` reads the
//! stream again and skips `offset` rows, so a read resumed after a transport error continues
//! with the same rows; it repeats the work of the skipped rows, as BigQuery's does.

use crate::catalog::{EngineCatalog, LoadedTable};
use crate::error::{EngineError, Resource};
use crate::ids::TableRef;
use crate::services::timestamp;
use crate::sql::SqlEngine;
use crate::types::{Column, Columns, IpcCompression, IpcEncoder, WireSchema};
use arrow::array::{AsArray, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use datafusion::common::DFSchema;
use datafusion::physical_expr::PhysicalExpr;
use futures::{StreamExt, TryStreamExt};
use gcloud_sdk::google::cloud::bigquery::storage::v1 as storage;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::expr::Predicate;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use iceberg_datafusion::physical_plan::convert_filters_to_predicate;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

/// The rows the reader decodes per batch: one `ReadRows` response each, split further when its
/// message would pass [`MESSAGE_LIMIT`].
const BATCH_ROWS: usize = 8192;

/// The largest serialized batch one `ReadRows` response carries.
const MESSAGE_LIMIT: usize = 16 * 1024 * 1024;

/// The open read sessions of one engine.
pub(crate) struct ReadSessions {
    catalog: Arc<EngineCatalog>,
    sql: Arc<SqlEngine>,
    ttl: Duration,
    sessions: Mutex<HashMap<Uuid, Arc<Session>>>,
}

impl std::fmt::Debug for ReadSessions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadSessions")
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

/// One session: what its streams read and how.
struct Session {
    name: String,
    file_io: FileIO,
    runtime: iceberg::Runtime,
    /// The columns the reader returns for the session's tasks, in this order.
    scan: SchemaRef,
    /// The residual restriction over `scan`, applied to every batch.
    filter: Option<Arc<dyn PhysicalExpr>>,
    /// The positions in `scan` of the columns the session returns.
    selected: Vec<usize>,
    wire: WireSchema,
    compression: Option<IpcCompression>,
    streams: Vec<Vec<FileScanTask>>,
    expires: Instant,
}

/// A stream of a session, as its name names it.
struct StreamName {
    session: Uuid,
    index: usize,
}

impl std::str::FromStr for StreamName {
    type Err = EngineError;

    /// `projects/{project}/locations/{location}/sessions/{session}/streams/{index}`.
    fn from_str(name: &str) -> Result<Self, Self::Err> {
        let not_found = || EngineError::NotFound {
            what: Resource::ReadStream(name.to_string()),
        };
        let (session, index) = name
            .split_once("/sessions/")
            .and_then(|(_, rest)| rest.split_once("/streams/"))
            .ok_or_else(not_found)?;
        Ok(Self {
            session: session.parse().map_err(|_| not_found())?,
            index: index.parse().map_err(|_| not_found())?,
        })
    }
}

impl ReadSessions {
    pub(crate) fn new(catalog: Arc<EngineCatalog>, sql: Arc<SqlEngine>, ttl: Duration) -> Self {
        Self {
            catalog,
            sql,
            ttl,
            sessions: Mutex::default(),
        }
    }

    /// Opens a session: pins the table's snapshot, plans the scan with the restriction's
    /// convertible part, and splits the tasks into streams.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] for a missing table, [`EngineError::Invalid`] for a selected
    /// field or a restriction column the table does not have, with `selected fields do not
    /// exist` in the message, and [`EngineError::Unsupported`] for Avro, nested selected fields
    /// and `sample_percentage`.
    pub(crate) async fn create(
        &self,
        request: storage::CreateReadSessionRequest,
    ) -> Result<storage::ReadSession, EngineError> {
        let requested = request
            .read_session
            .ok_or_else(|| EngineError::invalid("read_session", "the request carries none"))?;
        let table = TableRef::from_storage_path(&requested.table)?;
        if requested.data_format == storage::DataFormat::Avro as i32 {
            return Err(EngineError::unsupported(
                "Avro",
                "read sessions serve Arrow only",
            ));
        }
        let options = requested.read_options.clone().unwrap_or_default();
        if options.sample_percentage.is_some() {
            return Err(EngineError::unsupported(
                "sample_percentage",
                "set on a read session",
            ));
        }
        let compression = IpcCompression::of_read_options(&options);
        let loaded = self.catalog.load(&table).await?;
        let dataset = self.catalog.dataset(&table.dataset_ref()).await?;
        let snapshot_time = requested
            .table_modifiers
            .as_ref()
            .and_then(|modifiers| modifiers.snapshot_time);
        let pinned = Pinned::of(&loaded, &table, snapshot_time)?;
        let selected = pinned.selected(&options.selected_fields)?;
        let restriction = match options.row_restriction.trim() {
            "" => None,
            text => Some(self.restriction(&pinned, text)?),
        };
        let mut scan_columns: Vec<String> = selected
            .iter()
            .map(|column| column.name.to_string())
            .collect();
        if let Some(restriction) = &restriction {
            for column in &restriction.columns {
                if !scan_columns.contains(column) {
                    scan_columns.push(column.clone());
                }
            }
        }
        let tasks = pinned
            .plan(
                &scan_columns,
                restriction.as_ref().and_then(|r| r.predicate.clone()),
            )
            .await?;
        let estimated_bytes: u64 = tasks.iter().map(|task| task.length).sum();
        let estimated_rows: u64 = tasks.iter().filter_map(|task| task.record_count).sum();
        let streams = split_tasks(tasks, stream_count(request.max_stream_count));
        let scan = Arc::new(
            pinned
                .engine
                .project(
                    &scan_columns
                        .iter()
                        .map(|name| pinned.engine.index_of(name))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|err| EngineError::Internal {
                            detail: format!("a planned column is not in the table: {err}"),
                        })?,
                )
                .map_err(|err| EngineError::Internal {
                    detail: format!("the scan schema does not project: {err}"),
                })?,
        );
        let filter = match restriction {
            Some(restriction) => {
                let schema = DFSchema::try_from(scan.as_ref().clone())?;
                Some(
                    self.sql
                        .session()
                        .state()
                        .create_physical_expr(restriction.expr, &schema)?,
                )
            }
            None => None,
        };
        let wire = WireSchema::of_columns(selected.iter().copied());
        let schema_message = IpcEncoder::new(wire.schema(), compression)?
            .schema_message()
            .to_vec();
        let id = Uuid::new_v4();
        let name = format!(
            "projects/{}/locations/{}/sessions/{id}",
            table.project,
            dataset.location.as_str().to_ascii_lowercase()
        );
        let stream_names: Vec<String> = (0..streams.len())
            .map(|index| format!("{name}/streams/{index}"))
            .collect();
        let session = Session {
            name: name.clone(),
            file_io: loaded.table.file_io().clone(),
            runtime: self.catalog.iceberg_runtime(),
            selected: (0..selected.len()).collect(),
            scan,
            filter,
            wire,
            compression,
            streams,
            expires: Instant::now() + self.ttl,
        };
        {
            let mut sessions = self.sessions.lock().unwrap_or_else(PoisonError::into_inner);
            let now = Instant::now();
            sessions.retain(|_, session| session.expires > now);
            sessions.insert(id, Arc::new(session));
        }
        Ok(storage::ReadSession {
            name,
            expire_time: Some(timestamp(SystemTime::now() + self.ttl)),
            data_format: storage::DataFormat::Arrow.into(),
            table: requested.table,
            table_modifiers: requested.table_modifiers,
            read_options: requested.read_options,
            streams: stream_names
                .into_iter()
                .map(|name| storage::ReadStream { name })
                .collect(),
            estimated_total_bytes_scanned: i64::try_from(estimated_bytes).unwrap_or(i64::MAX),
            estimated_row_count: i64::try_from(estimated_rows).unwrap_or(i64::MAX),
            schema: Some(storage::read_session::Schema::ArrowSchema(
                storage::ArrowSchema {
                    serialized_schema: schema_message,
                },
            )),
            ..Default::default()
        })
    }

    /// The rows of one stream from `offset` on, one response per batch.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] for a stream of no live session; the stream itself ends with
    /// [`EngineError::OutOfRange`] when `offset` is past its last row.
    pub(crate) fn read(
        &self,
        request: storage::ReadRowsRequest,
    ) -> Result<ReceiverStream<Result<storage::ReadRowsResponse, EngineError>>, EngineError> {
        let stream: StreamName = request.read_stream.parse()?;
        let session = {
            let sessions = self.sessions.lock().unwrap_or_else(PoisonError::into_inner);
            sessions
                .get(&stream.session)
                .filter(|session| session.expires > Instant::now())
                .cloned()
        }
        .filter(|session| stream.index < session.streams.len())
        .ok_or_else(|| EngineError::NotFound {
            what: Resource::ReadStream(request.read_stream.clone()),
        })?;
        let offset = u64::try_from(request.offset).map_err(|_| {
            EngineError::invalid("offset", format!("{} is negative", request.offset))
        })?;
        let (sender, receiver) = mpsc::channel(2);
        tokio::spawn(async move {
            if let Err(err) = session.pump(stream.index, offset, &sender).await {
                let _ = sender.send(Err(err)).await;
            }
        });
        Ok(ReceiverStream::new(receiver))
    }

    /// The restriction's expression over every column of the pinned schema, its convertible
    /// part as an Iceberg predicate, and the columns it reads.
    fn restriction(&self, pinned: &Pinned, text: &str) -> Result<Restriction, EngineError> {
        let schema = DFSchema::try_from(pinned.engine.as_ref().clone())?;
        let expr = crate::sql::restriction(&self.sql.session(), text, &schema)?;
        let columns = expr
            .column_refs()
            .into_iter()
            .map(|column| column.name.clone())
            .collect();
        Ok(Restriction {
            predicate: convert_filters_to_predicate(std::slice::from_ref(&expr)),
            expr,
            columns,
        })
    }
}

/// A parsed row restriction.
struct Restriction {
    expr: datafusion::logical_expr::Expr,
    predicate: Option<Predicate>,
    columns: Vec<String>,
}

/// A table pinned at one snapshot: its columns and engine schema as of that snapshot.
struct Pinned<'t> {
    loaded: &'t LoadedTable,
    snapshot_id: Option<i64>,
    columns: Columns,
    engine: SchemaRef,
}

impl<'t> Pinned<'t> {
    /// `loaded` at the snapshot current at `snapshot_time`, or at its current snapshot.
    fn of(
        loaded: &'t LoadedTable,
        table: &TableRef,
        snapshot_time: Option<gcloud_sdk::prost_types::Timestamp>,
    ) -> Result<Self, EngineError> {
        let metadata = loaded.metadata();
        let snapshot_id = match snapshot_time {
            None => metadata.current_snapshot_id(),
            Some(time) => {
                let at_ms = time
                    .seconds
                    .saturating_mul(1000)
                    .saturating_add(i64::from(time.nanos) / 1_000_000);
                let current = metadata
                    .history()
                    .iter()
                    .filter(|entry| entry.timestamp_ms <= at_ms)
                    .max_by_key(|entry| entry.timestamp_ms);
                match current {
                    Some(entry) => Some(entry.snapshot_id),
                    None => {
                        return Err(EngineError::invalid(
                            "table_modifiers.snapshot_time",
                            format!("{table} has no snapshot at or before {at_ms} ms"),
                        ))
                    }
                }
            }
        };
        let schema = match snapshot_id.and_then(|id| metadata.snapshot_by_id(id)) {
            Some(snapshot) => snapshot.schema(metadata)?,
            None => metadata.current_schema().clone(),
        };
        Ok(Self {
            loaded,
            snapshot_id,
            columns: Columns::from_iceberg(&schema, metadata.properties())?,
            engine: Arc::new(iceberg::arrow::schema_to_arrow_schema(&schema)?),
        })
    }

    /// The columns `selected` names, in table order; every column when it names none.
    fn selected(&self, selected: &[String]) -> Result<Vec<&Column>, EngineError> {
        if let Some(nested) = selected.iter().find(|name| name.contains('.')) {
            return Err(EngineError::unsupported(
                "nested selected_fields",
                format!("`{nested}` names a field of a column"),
            ));
        }
        if let Some(unknown) = selected
            .iter()
            .find(|name| self.columns.get(name).is_none())
        {
            return Err(EngineError::invalid(
                "read_options.selected_fields",
                format!("selected fields do not exist: {unknown}"),
            ));
        }
        Ok(self
            .columns
            .iter()
            .filter(|column| {
                selected.is_empty() || selected.iter().any(|name| name == column.name.as_str())
            })
            .collect())
    }

    /// The file tasks of the scan of `columns` at the pinned snapshot, pruned by `predicate`;
    /// none for a table without a snapshot.
    async fn plan(
        &self,
        columns: &[String],
        predicate: Option<Predicate>,
    ) -> Result<Vec<FileScanTask>, EngineError> {
        let Some(snapshot_id) = self.snapshot_id else {
            return Ok(Vec::new());
        };
        let mut scan = self
            .loaded
            .table
            .scan()
            .snapshot_id(snapshot_id)
            .select(columns.iter().cloned());
        if let Some(predicate) = predicate {
            scan = scan.with_filter(predicate);
        }
        Ok(scan.build()?.plan_files().await?.try_collect().await?)
    }
}

/// The streams a session splits into: as many as the caller allows, one per core when it sets no
/// limit.
fn stream_count(max_stream_count: i32) -> usize {
    match usize::try_from(max_stream_count) {
        Ok(count) if count > 0 => count,
        _ => std::thread::available_parallelism().map_or(1, usize::from),
    }
}

/// `tasks` in at most `count` streams, at least one: largest first, dealt round-robin, so that
/// the streams' sizes stay close. Ties fall back to the path and the offset, so the split of one
/// planned set never depends on the planner's order.
fn split_tasks(mut tasks: Vec<FileScanTask>, count: usize) -> Vec<Vec<FileScanTask>> {
    tasks.sort_by(|a, b| {
        b.length
            .cmp(&a.length)
            .then_with(|| a.data_file_path.cmp(&b.data_file_path))
            .then_with(|| a.start.cmp(&b.start))
    });
    let count = count.clamp(1, tasks.len().max(1));
    let mut streams = vec![Vec::new(); count];
    for (index, task) in tasks.into_iter().enumerate() {
        streams[index % count].push(task);
    }
    streams
}

impl Session {
    /// Sends the stream's rows from `offset` on, one response per batch, until the stream ends
    /// or the receiver goes away.
    async fn pump(
        &self,
        index: usize,
        offset: u64,
        sender: &mpsc::Sender<Result<storage::ReadRowsResponse, EngineError>>,
    ) -> Result<(), EngineError> {
        let tasks = self.streams[index].clone();
        let reader = ArrowReaderBuilder::new(self.file_io.clone(), self.runtime.clone())
            .with_data_file_concurrency_limit(1)
            .with_batch_size(BATCH_ROWS)
            .build();
        let mut batches = reader
            .read(futures::stream::iter(tasks.into_iter().map(Ok)).boxed())?
            .stream();
        let mut skip = offset;
        let mut encoder = IpcEncoder::new(self.wire.schema(), self.compression)?;
        while let Some(batch) = batches.try_next().await? {
            let batch = self.rows(&batch)?;
            let rows = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
            if skip >= rows {
                skip -= rows;
                continue;
            }
            let start = usize::try_from(skip).unwrap_or(usize::MAX);
            skip = 0;
            let batch = batch.slice(start, batch.num_rows() - start);
            let wire = self.wire.normalize(&batch)?;
            for response in encode_responses(&mut encoder, &wire)? {
                if sender.send(Ok(response)).await.is_err() {
                    return Ok(());
                }
            }
        }
        if skip > 0 {
            return Err(EngineError::OutOfRange {
                what: Resource::ReadStream(format!("{}/streams/{index}", self.name)),
                detail: format!("offset {offset} is past the stream's last row"),
            });
        }
        Ok(())
    }

    /// One batch of the reader in the scan's column order, restricted, with the session's
    /// columns only.
    fn rows(&self, batch: &RecordBatch) -> Result<RecordBatch, EngineError> {
        let schema = batch.schema();
        let order = self
            .scan
            .fields()
            .iter()
            .map(|field| schema.index_of(field.name()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| EngineError::Internal {
                detail: format!("the reader left out a planned column: {err}"),
            })?;
        let columns = order.iter().map(|i| batch.column(*i).clone()).collect();
        let scanned = RecordBatch::try_new(self.scan.clone(), columns).map_err(|err| {
            EngineError::Internal {
                detail: format!("the reader's batch does not match the scan: {err}"),
            }
        })?;
        let restricted = match &self.filter {
            Some(filter) => {
                let mask = filter.evaluate(&scanned)?.into_array(scanned.num_rows())?;
                filter_record_batch(&scanned, mask.as_boolean()).map_err(|err| {
                    EngineError::Internal {
                        detail: format!("the restriction did not apply: {err}"),
                    }
                })?
            }
            None => scanned,
        };
        restricted
            .project(&self.selected)
            .map_err(|err| EngineError::Internal {
                detail: format!("the session's columns do not project: {err}"),
            })
    }
}

/// The responses of one batch: one, or several when its message would pass the limit.
fn encode_responses(
    encoder: &mut IpcEncoder,
    batch: &RecordBatch,
) -> Result<Vec<storage::ReadRowsResponse>, EngineError> {
    let message = encoder.encode(batch)?;
    if message.len() > MESSAGE_LIMIT && batch.num_rows() > 1 {
        let half = batch.num_rows() / 2;
        let mut responses = encode_responses(encoder, &batch.slice(0, half))?;
        responses.extend(encode_responses(
            encoder,
            &batch.slice(half, batch.num_rows() - half),
        )?);
        return Ok(responses);
    }
    let rows = i64::try_from(batch.num_rows()).unwrap_or(i64::MAX);
    Ok(vec![storage::ReadRowsResponse {
        row_count: rows,
        uncompressed_byte_size: i64::try_from(batch.get_array_memory_size()).ok(),
        rows: Some(storage::read_rows_response::Rows::ArrowRecordBatch(
            storage::ArrowRecordBatch {
                serialized_record_batch: message,
                ..Default::default()
            },
        )),
        ..Default::default()
    }])
}

impl IpcCompression {
    /// The compression a read session's Arrow options ask for, `None` for none.
    fn of_read_options(options: &storage::read_session::TableReadOptions) -> Option<Self> {
        use storage::arrow_serialization_options::CompressionCodec;
        use storage::read_session::table_read_options::OutputFormatSerializationOptions;
        match &options.output_format_serialization_options {
            Some(OutputFormatSerializationOptions::ArrowSerializationOptions(arrow)) => {
                match CompressionCodec::try_from(arrow.buffer_compression) {
                    Ok(CompressionCodec::Lz4Frame) => Some(IpcCompression::Lz4Frame),
                    Ok(CompressionCodec::Zstd) => Some(IpcCompression::Zstd),
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;
