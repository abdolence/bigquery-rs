//! The write streams `CreateWriteStream` makes, each with offsets, so that a request sent again
//! lands once:
//!
//! - a committed stream commits each request as it comes, through the table's commit group;
//! - a pending stream stages each request as a segment and commits every segment at
//!   `BatchCommitWriteStreams`, together with the other streams named there, in one commit;
//! - a buffered stream stages each request as a segment and commits the segments up to the
//!   offset of each `FlushRows`.
//!
//! Only the engine that created a stream serves its appends, flushes and finalize, since only it
//! knows the stream's next offset. A batch commit and `GetWriteStream` read the stream from
//! storage and from its table, so any engine on the root serves them.
//!
//! A pending or buffered stream's state lives in its table's properties, from the commit that
//! registers the stream to the commit that drops it, and every commit of its rows changes that
//! state only from the state it expects. The pointer chain therefore fences every engine's
//! commits of the stream against every other's, and the drop of an expired stream, by whichever
//! engine sweeps the root, lands before anything of the stream is deleted: the creator's next
//! commit of the stream finds no state and is refused.
//!
//! Each stream has a lock in its creator, held across every change of its state and taken before
//! the table's commit lock. A commit whose outcome is unknown stops the stream: whatever comes
//! next is refused rather than risk a second copy.

use crate::catalog::{now_ms, Change, EngineCatalog, EngineCommit, LoadedTable};
use crate::error::{EngineError, Resource, StreamProblem};
use crate::ids::{NodeId, RowOffset, StreamId, TableRef, WriteStreamId, WriteStreamName};
use crate::storage::{
    ListedSegment, SegmentFiles, StagedFile, Staging, StoredStream, StreamChange, StreamMode,
    StreamRecord, StreamState,
};
use crate::write::{proto_time, CommitGroups, TableWriter, WriteDisposition, WrittenFiles};
use arrow::array::RecordBatch;
use gcloud_sdk::google::cloud::bigquery::storage::v1 as storage;
use iceberg::TableUpdate;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime};
use storage::write_stream::Type;
use tokio::sync::OwnedMutexGuard;
use tracing::{debug, warn};

/// The created write streams of one engine.
pub(crate) struct Streams {
    catalog: Arc<EngineCatalog>,
    groups: Arc<CommitGroups>,
    staging: Staging,
    ttl: Duration,
    node: NodeId,
    created: Mutex<HashMap<WriteStreamId, Arc<tokio::sync::Mutex<Stream>>>>,
    /// When the last sweep started, held while a sweep runs.
    sweeping: Arc<tokio::sync::Mutex<Option<Instant>>>,
}

impl std::fmt::Debug for Streams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Streams")
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

/// A stream this engine created.
struct Stream {
    id: WriteStreamId,
    record: StreamRecord,
    /// When this engine last started writing the record. The backend's creation time of the
    /// record, which a sweep measures the TTL from, is no earlier.
    written: Instant,
    /// The offset the next append takes.
    next: RowOffset,
    finalized: bool,
    /// A buffered stream's last flushed offset.
    flushed: Option<RowOffset>,
    /// Set once a commit of the stream's rows ended with an unknown outcome.
    failed: bool,
}

/// What `GetWriteStream` reports of a created stream.
pub(crate) struct StreamDescription {
    pub kind: Type,
    pub created_at_ms: i64,
    pub committed_at_ms: Option<i64>,
}

/// # Errors
/// [`EngineError::Invalid`] for a type that is not committed, pending or buffered.
impl TryFrom<Type> for StreamMode {
    type Error = EngineError;

    fn try_from(kind: Type) -> Result<Self, Self::Error> {
        match kind {
            Type::Committed => Ok(StreamMode::Committed),
            Type::Pending => Ok(StreamMode::Pending),
            Type::Buffered => Ok(StreamMode::Buffered),
            Type::Unspecified => Err(EngineError::invalid(
                "write_stream.type",
                "a created stream is COMMITTED, PENDING or BUFFERED",
            )),
        }
    }
}

impl From<StreamMode> for Type {
    fn from(mode: StreamMode) -> Self {
        match mode {
            StreamMode::Committed => Type::Committed,
            StreamMode::Pending => Type::Pending,
            StreamMode::Buffered => Type::Buffered,
        }
    }
}

impl StreamRecord {
    /// Refuses a table that is not the identity the stream writes into.
    fn check_identity(&self, loaded: &LoadedTable) -> Result<(), EngineError> {
        if loaded.metadata().uuid() == self.table_uuid {
            Ok(())
        } else {
            Err(EngineError::NotFound {
                what: Resource::Table(self.table.clone()),
            })
        }
    }
}

impl WriteStreamName {
    /// The ID of the created stream this names, which `request` applies to.
    fn created(&self, request: &'static str) -> Result<WriteStreamId, EngineError> {
        match self.stream {
            StreamId::Created(id) => Ok(id),
            StreamId::Default => Err(EngineError::stream(
                self,
                StreamProblem::WrongType {
                    request,
                    applies_to: "created streams",
                },
            )),
        }
    }
}

impl Stream {
    fn name(&self) -> WriteStreamName {
        WriteStreamName {
            table: self.record.table.clone(),
            stream: StreamId::Created(self.id),
        }
    }

    /// Refuses an append at `offset` the stream cannot take.
    fn check_append(&self, offset: RowOffset) -> Result<(), EngineError> {
        let problem = if self.failed {
            StreamProblem::OutcomeUnknown
        } else if self.finalized {
            StreamProblem::Finalized
        } else if offset < self.next {
            StreamProblem::OffsetAlreadyExists {
                offset,
                next: self.next,
            }
        } else if offset > self.next {
            StreamProblem::OffsetOutOfRange {
                offset,
                next: self.next,
            }
        } else {
            return Ok(());
        };
        Err(EngineError::stream(&self.name(), problem))
    }

    /// Refuses an append to a pending or buffered stream that `loaded` no longer holds open,
    /// whose rows no commit would take.
    fn check_open(&self, loaded: &LoadedTable) -> Result<(), EngineError> {
        if self.record.mode == StreamMode::Committed {
            return Ok(());
        }
        let problem = match StoredStream::read(self.id, loaded.metadata().properties())? {
            Some(stored) if stored.state == StreamState::Open => return Ok(()),
            Some(_) => StreamProblem::ChangedElsewhere,
            None => StreamProblem::Expired,
        };
        Err(EngineError::stream(&self.name(), problem))
    }
}

impl Streams {
    pub(crate) fn new(
        catalog: Arc<EngineCatalog>,
        groups: Arc<CommitGroups>,
        ttl: Duration,
    ) -> Self {
        Self {
            staging: Staging::new(catalog.backend().clone()),
            catalog,
            groups,
            ttl,
            node: NodeId::random(),
            created: Mutex::default(),
            sweeping: Arc::default(),
        }
    }

    /// Creates a stream of `kind` on `table`, whose record is on storage, and a pending or
    /// buffered stream's state in its table, when this returns.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] for a missing table, [`EngineError::Invalid`] for a type
    /// that is not committed, pending or buffered.
    pub(crate) async fn create(
        &self,
        table: &TableRef,
        kind: Type,
    ) -> Result<WriteStreamName, EngineError> {
        let mode = StreamMode::try_from(kind)?;
        let loaded = self.catalog.load(table).await?;
        let id = WriteStreamId::random();
        let record = StreamRecord {
            mode,
            table: table.clone(),
            table_uuid: loaded.metadata().uuid(),
            created_at_ms: now_ms(),
            creator: self.node,
        };
        let written = Instant::now();
        self.staging.write_record(id, &record).await?;
        if mode != StreamMode::Committed {
            self.groups
                .commit_streams(
                    table,
                    WrittenFiles::none(record.table_uuid),
                    vec![StreamChange::Register(id)],
                )
                .await?;
        }
        let stream = Stream {
            id,
            record,
            written,
            next: RowOffset::default(),
            finalized: false,
            flushed: None,
            failed: false,
        };
        let name = stream.name();
        self.created
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id, Arc::new(tokio::sync::Mutex::new(stream)));
        Ok(name)
    }

    /// What `GetWriteStream` reports of the stream.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] for a stream that does not exist.
    pub(crate) async fn describe(
        &self,
        name: &WriteStreamName,
        loaded: &LoadedTable,
    ) -> Result<StreamDescription, EngineError> {
        let id = name.created("GetWriteStream")?;
        let record = self.record(name, id).await?;
        let stored = match record.check_identity(loaded) {
            Ok(()) => StoredStream::read(id, loaded.metadata().properties())?,
            Err(_) => None,
        };
        Ok(StreamDescription {
            kind: record.mode.into(),
            created_at_ms: record.created_at_ms,
            committed_at_ms: stored.and_then(|stored| stored.committed_at_ms),
        })
    }

    /// Appends `batch`, rows of `loaded` in its engine types, at `offset`, and returns the
    /// offset. A committed stream's rows are visible once this returns; a pending or buffered
    /// stream's are staged as a segment.
    ///
    /// # Errors
    /// [`EngineError::WriteStream`] for an offset or a state the stream refuses,
    /// [`EngineError::NotFound`] for a stream that does not exist or a table that is not the
    /// one the stream was created on, and the errors of the write and the commit.
    pub(crate) async fn append(
        &self,
        name: &WriteStreamName,
        offset: RowOffset,
        loaded: &LoadedTable,
        batch: RecordBatch,
    ) -> Result<RowOffset, EngineError> {
        let mut stream = self.lock(name, "AppendRows").await?;
        stream.check_append(offset)?;
        stream.record.check_identity(loaded)?;
        stream.check_open(loaded)?;
        let rows = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
        if rows == 0 {
            return Ok(offset);
        }
        let writer = TableWriter::new(loaded)?;
        if stream.record.mode == StreamMode::Committed {
            let files = writer.write([batch]).await?;
            let landed = self
                .groups
                .append(&stream.record.table, files, WriteDisposition::Append)
                .await;
            if let Err(err) = landed {
                stream.failed |= err.commit_may_have_landed();
                return Err(err);
            }
            self.touch(&mut stream).await;
        } else {
            let files = writer.write_staged(batch, offset).await?;
            self.staging
                .write_segment(
                    stream.id,
                    offset,
                    offset.after(rows - 1),
                    &SegmentFiles { files },
                )
                .await?;
        }
        stream.next = offset.after(rows);
        Ok(offset)
    }

    /// Finalizes the stream, which then takes no more rows, and returns the offset after its
    /// last row. A pending stream is finalized in its table, where a batch commit on any engine
    /// finds it; the others in their creator only. Finalizing a finalized stream changes nothing.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] for a stream that does not exist or whose table is not the
    /// identity it writes into, and the errors of the pending stream's commit.
    pub(crate) async fn finalize(&self, name: &WriteStreamName) -> Result<RowOffset, EngineError> {
        let mut stream = self.lock(name, "FinalizeWriteStream").await?;
        if !stream.finalized && stream.record.mode == StreamMode::Pending {
            if stream.failed {
                return Err(EngineError::stream(name, StreamProblem::OutcomeUnknown));
            }
            let landed = self
                .groups
                .commit_streams(
                    &stream.record.table,
                    WrittenFiles::none(stream.record.table_uuid),
                    vec![StreamChange::Finalize(stream.id)],
                )
                .await;
            if let Err(err) = landed {
                stream.failed |= err.commit_may_have_landed();
                return Err(err);
            }
        }
        stream.finalized = true;
        Ok(stream.next)
    }

    /// Commits every row of the buffered stream up to and including `offset`, and returns
    /// `offset`. A flush at an offset already flushed commits nothing.
    ///
    /// The segment with rows on both sides of `offset` is first written again as two segments,
    /// one per side, both before the commit; the commit then takes the lower one, and the
    /// original goes once the commit has landed.
    ///
    /// # Errors
    /// [`EngineError::WriteStream`] for a stream that is not buffered, whose last commit's
    /// outcome is unknown or that was dropped, [`EngineError::OutOfRange`] for an offset past
    /// the stream's rows, and the errors of the split and the commit.
    pub(crate) async fn flush(
        &self,
        name: &WriteStreamName,
        offset: RowOffset,
    ) -> Result<RowOffset, EngineError> {
        let mut stream = self.lock(name, "FlushRows").await?;
        if stream.record.mode != StreamMode::Buffered {
            return Err(EngineError::stream(
                name,
                StreamProblem::WrongType {
                    request: "FlushRows",
                    applies_to: "BUFFERED streams",
                },
            ));
        }
        if stream.failed {
            return Err(EngineError::stream(name, StreamProblem::OutcomeUnknown));
        }
        if offset >= stream.next {
            return Err(EngineError::OutOfRange {
                what: Resource::WriteStream(Box::new(name.clone())),
                detail: format!(
                    "offset {offset} is past the stream's rows, which end before offset {}",
                    stream.next
                ),
            });
        }
        let flushed = stream.flushed;
        if flushed.is_some_and(|flushed| offset <= flushed) {
            return Ok(offset);
        }
        let table = stream.record.table.clone();
        let loaded = self.catalog.load(&table).await?;
        stream.record.check_identity(&loaded)?;

        let mut committing = Vec::new();
        let mut straddling = None;
        for segment in self.select(name, stream.id, flushed).await? {
            if segment.first > offset {
                break;
            }
            let staged = self.staged(name, stream.id, &segment).await?;
            if segment.last <= offset {
                committing.extend(staged.files);
            } else {
                let (lower, rewritten) = self
                    .split(stream.id, &loaded, &segment, staged, offset)
                    .await?;
                committing.extend(lower);
                straddling = Some((segment, rewritten));
            }
        }
        let change = StreamChange::Flush {
            stream: stream.id,
            from: flushed,
            to: offset,
        };
        let landed = self
            .groups
            .commit_streams(
                &table,
                WrittenFiles::staged(&committing, &loaded)?,
                vec![change],
            )
            .await;
        if let Err(err) = landed {
            stream.failed |= err.commit_may_have_landed();
            return Err(err);
        }
        stream.flushed = Some(offset);
        if let Some((segment, rewritten)) = straddling {
            self.staging.delete_segment(stream.id, &segment).await;
            for path in rewritten {
                self.staging.delete_data_file(&path).await;
            }
        }
        self.touch(&mut stream).await;
        Ok(offset)
    }

    /// Writes the record of a stream whose rows a commit just took again, once an eighth of the
    /// TTL has passed since it was last written. A sweep drops a stream whose record the backend
    /// created a TTL ago, so a committed or buffered stream lives the TTL from its last commit,
    /// and a pending stream, whose rows commit once, the TTL from its creation.
    async fn touch(&self, stream: &mut Stream) {
        if stream.written.elapsed() < self.ttl / 8 {
            return;
        }
        let written = Instant::now();
        match self.staging.write_record(stream.id, &stream.record).await {
            Ok(()) => stream.written = written,
            Err(err) => {
                warn!(stream = %stream.id, %err, "A write stream's record could not be written again.");
            }
        }
    }

    /// Writes `segment`, whose rows straddle `offset`, again as two segments: its rows up to
    /// and including `offset`, and the rest. A file with rows on both sides is rewritten as
    /// two; any other goes to its side as it is. Returns the files of the lower segment and the
    /// paths of the files rewritten, which no segment names once the original is gone.
    async fn split(
        &self,
        stream: WriteStreamId,
        loaded: &LoadedTable,
        segment: &ListedSegment,
        staged: SegmentFiles,
        offset: RowOffset,
    ) -> Result<(Vec<StagedFile>, Vec<String>), EngineError> {
        let end = offset.after(1);
        let writer = TableWriter::new(loaded)?;
        let mut lower = Vec::new();
        let mut upper = Vec::new();
        let mut rewritten = Vec::new();
        for file in staged.files {
            let below = file.rows.rows_before(end);
            if below == file.rows.rows() {
                lower.push(file);
            } else if below == 0 {
                upper.push(file);
            } else {
                let (below, above) = writer.split(&file, end).await?;
                lower.extend(below);
                upper.extend(above);
                rewritten.push(file.path);
            }
        }
        let lower = SegmentFiles { files: lower };
        self.staging
            .write_segment(stream, segment.first, offset, &lower)
            .await?;
        self.staging
            .write_segment(stream, end, segment.last, &SegmentFiles { files: upper })
            .await?;
        Ok((lower.files, rewritten))
    }

    /// Commits every named pending stream of `table` in one commit, or none of them: a stream
    /// that is not a finalized, uncommitted pending stream of the table is reported in the
    /// response's `stream_errors`, and then nothing is committed.
    ///
    /// # Errors
    /// [`EngineError::Invalid`] for a name that is no write stream's, and the errors of the
    /// commit.
    pub(crate) async fn batch_commit(
        &self,
        table: &TableRef,
        names: &[String],
    ) -> Result<storage::BatchCommitWriteStreamsResponse, EngineError> {
        let mut named = Vec::new();
        let mut refused = Vec::new();
        for text in names {
            let name: WriteStreamName = text.parse()?;
            if name.table == *table {
                named.push(name);
            } else {
                refused.push(EngineError::stream_not_found(&name));
            }
        }
        named.sort_by_key(ToString::to_string);
        named.dedup();
        let mut streams = Vec::new();
        for name in named {
            match self.pending(&name).await {
                Ok((id, record)) => streams.push((name, id, record)),
                Err(err @ (EngineError::NotFound { .. } | EngineError::WriteStream { .. })) => {
                    refused.push(err);
                }
                Err(err) => return Err(err),
            }
        }
        if !refused.is_empty() {
            return Ok(Self::refused(&refused));
        }
        let loaded = self.catalog.load(table).await?;
        let committed_at_ms = now_ms();
        let mut changes = Vec::new();
        for (name, id, record) in &streams {
            record.check_identity(&loaded)?;
            let change = StreamChange::Commit {
                stream: *id,
                at_ms: committed_at_ms,
            };
            let stored = StoredStream::read(*id, loaded.metadata().properties())?;
            if let Err(problem) = change.apply(stored) {
                refused.push(EngineError::stream(name, problem));
            }
            changes.push(change);
        }
        if !refused.is_empty() {
            return Ok(Self::refused(&refused));
        }

        let mut staged = Vec::new();
        for (name, id, _) in &streams {
            for segment in self.select(name, *id, None).await? {
                staged.extend(self.staged(name, *id, &segment).await?.files);
            }
        }
        let files = WrittenFiles::staged(&staged, &loaded)?;
        match self.groups.commit_streams(table, files, changes).await {
            Ok(()) => Ok(storage::BatchCommitWriteStreamsResponse {
                commit_time: proto_time(committed_at_ms),
                stream_errors: Vec::new(),
            }),
            Err(err @ EngineError::WriteStream { .. }) => Ok(Self::refused(&[err])),
            Err(err) => Err(err),
        }
    }

    /// The pending stream `name` names, which a batch commit takes.
    async fn pending(
        &self,
        name: &WriteStreamName,
    ) -> Result<(WriteStreamId, StreamRecord), EngineError> {
        let id = name.created("BatchCommitWriteStreams")?;
        let record = self.record(name, id).await?;
        if record.mode == StreamMode::Pending {
            Ok((id, record))
        } else {
            Err(EngineError::stream(
                name,
                StreamProblem::WrongType {
                    request: "BatchCommitWriteStreams",
                    applies_to: "PENDING streams",
                },
            ))
        }
    }

    /// A batch commit's answer when it commits nothing because of `refused`.
    fn refused(refused: &[EngineError]) -> storage::BatchCommitWriteStreamsResponse {
        storage::BatchCommitWriteStreamsResponse {
            commit_time: None,
            stream_errors: refused
                .iter()
                .filter_map(EngineError::stream_error)
                .collect(),
        }
    }

    /// The segments a commit of the stream takes, in offset order: those above `flushed`, one
    /// per range.
    ///
    /// # Errors
    /// [`EngineError::WriteStream`] when the segments do not line up, which no order of the
    /// engine's writes produces, and the storage errors of the listing.
    async fn select(
        &self,
        name: &WriteStreamName,
        stream: WriteStreamId,
        flushed: Option<RowOffset>,
    ) -> Result<Vec<ListedSegment>, EngineError> {
        self.staging
            .segments(stream)
            .await?
            .select(flushed)
            .map_err(|segment| {
                warn!(%stream, segment = segment.name, "A write stream's segments overlap.");
                EngineError::stream(name, StreamProblem::ChangedElsewhere)
            })
    }

    /// What `segment` of the stream holds. A segment listed and then gone was deleted with its
    /// stream, which a sweep dropped meanwhile.
    async fn staged(
        &self,
        name: &WriteStreamName,
        stream: WriteStreamId,
        segment: &ListedSegment,
    ) -> Result<SegmentFiles, EngineError> {
        self.staging
            .read_segment(stream, segment)
            .await?
            .ok_or_else(|| EngineError::stream(name, StreamProblem::Expired))
    }

    /// Starts a sweep on a task of its own, unless one is running or the last one started less
    /// than an eighth of the TTL ago, so that no request waits for a sweep and a busy engine
    /// lists the root's streams a few times per TTL only.
    pub(crate) fn sweep_soon(self: &Arc<Self>) {
        let Ok(mut started) = Arc::clone(&self.sweeping).try_lock_owned() else {
            return;
        };
        if started.is_some_and(|started| started.elapsed() < self.ttl / 8) {
            return;
        }
        *started = Some(Instant::now());
        let streams = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(err) = streams.sweep_expired().await {
                warn!(%err, "The expired write streams could not all be dropped.");
            }
            // Moved into the task and held to its end, so that no other sweep starts meanwhile.
            drop(started);
        });
    }

    /// Sweeps once the sweep running, if any, has finished.
    ///
    /// # Errors
    /// As [`Streams::sweep_expired`].
    #[cfg(test)]
    pub(crate) async fn sweep(&self) -> Result<(), EngineError> {
        let _running = self.sweeping.lock().await;
        self.sweep_expired().await
    }

    /// Drops every stream whose record the backend created more than the stream TTL ago: its
    /// state leaves its table first, so that no later commit can take its files, then the files
    /// no commit took, its segments and its record are deleted. This engine also stops serving
    /// its own streams past the TTL.
    ///
    /// # Errors
    /// The storage errors of listing the records; a stream that cannot be dropped is logged
    /// and left for the next sweep.
    async fn sweep_expired(&self) -> Result<(), EngineError> {
        let now = SystemTime::now();
        for (id, created) in self.staging.records().await? {
            if now
                .duration_since(created)
                .map_or(true, |age| age < self.ttl)
            {
                continue;
            }
            match self.expire(id).await {
                Ok(()) => debug!(stream = %id, "Dropped a write stream past its TTL."),
                Err(err) => {
                    warn!(stream = %id, %err, "An expired write stream could not be dropped.");
                }
            }
        }
        self.created
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|_, stream| {
                stream
                    .try_lock()
                    .map_or(true, |stream| stream.written.elapsed() < self.ttl)
            });
        Ok(())
    }

    /// Deletes the stream `id` from storage, its state in its table first.
    async fn expire(&self, id: WriteStreamId) -> Result<(), EngineError> {
        if let Some(record) = self.staging.read_record(id).await? {
            let committed = self.release(id, &record).await?;
            let mut segments = Vec::new();
            for segment in self.staging.segments(id).await? {
                if let Some(files) = self.staging.read_segment(id, &segment).await? {
                    segments.push((segment, files));
                }
            }
            for path in committed.uncommitted_files(&segments) {
                self.staging.delete_data_file(&path).await;
            }
        }
        self.created
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&id);
        self.staging.delete_stream(id).await
    }

    /// Removes the stream's state from its table, and returns what the state said the stream
    /// had committed.
    async fn release(
        &self,
        id: WriteStreamId,
        record: &StreamRecord,
    ) -> Result<CommittedRows, EngineError> {
        if record.mode == StreamMode::Committed {
            return Ok(CommittedRows::Everything);
        }
        let mut committed = CommittedRows::Unknown;
        let released = self
            .catalog
            .commit(&record.table, |loaded| {
                committed = CommittedRows::Unknown;
                if record.check_identity(loaded).is_err() {
                    return Ok(Change::Engine(EngineCommit::default()));
                }
                let properties = loaded.metadata().properties();
                if let Some(stored) = StoredStream::read(id, properties)? {
                    committed = match (record.mode, stored.flushed) {
                        (StreamMode::Buffered, Some(flushed)) => CommittedRows::Through(flushed),
                        _ if stored.state == StreamState::Committed => CommittedRows::Everything,
                        _ => CommittedRows::Nothing,
                    };
                }
                let removals: Vec<String> = StoredStream::keys(id)
                    .into_iter()
                    .filter(|key| properties.contains_key(key))
                    .collect();
                Ok(Change::Engine(EngineCommit {
                    requirements: Vec::new(),
                    updates: if removals.is_empty() {
                        Vec::new()
                    } else {
                        vec![TableUpdate::RemoveProperties { removals }]
                    },
                }))
            })
            .await;
        match released {
            Ok(_) => Ok(committed),
            Err(EngineError::NotFound { .. }) => Ok(CommittedRows::Unknown),
            Err(err) => Err(err),
        }
    }

    /// The stream `name` names, locked, for `request`, which only the stream's creator serves.
    async fn lock(
        &self,
        name: &WriteStreamName,
        request: &'static str,
    ) -> Result<OwnedMutexGuard<Stream>, EngineError> {
        let id = name.created(request)?;
        let created = self
            .created
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&id)
            .cloned();
        if let Some(stream) = created {
            let stream = stream.lock_owned().await;
            if stream.written.elapsed() >= self.ttl {
                self.created
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&id);
            } else if stream.record.table == name.table {
                return Ok(stream);
            }
        }
        Err(self.not_served(name, id).await?)
    }

    /// Why this engine does not serve the stream `name` names.
    async fn not_served(
        &self,
        name: &WriteStreamName,
        id: WriteStreamId,
    ) -> Result<EngineError, EngineError> {
        Ok(match self.staging.read_record(id).await? {
            Some(record) if record.table == name.table && record.creator != self.node => {
                EngineError::stream(name, StreamProblem::CreatedElsewhere)
            }
            _ => EngineError::stream_not_found(name),
        })
    }

    /// The record of the stream `name` names, whose ID is `id`.
    async fn record(
        &self,
        name: &WriteStreamName,
        id: WriteStreamId,
    ) -> Result<StreamRecord, EngineError> {
        match self.staging.read_record(id).await? {
            Some(record) if record.table == name.table => Ok(record),
            _ => Err(EngineError::stream_not_found(name)),
        }
    }
}

/// What a dropped stream committed, as its table's state said when it was removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommittedRows {
    Nothing,
    /// The rows up to and including the offset.
    Through(RowOffset),
    Everything,
    /// The table holds no state of the stream: it is gone, is another identity, or another
    /// sweep removed the state first.
    Unknown,
}

impl CommittedRows {
    /// The files of `segments` that no commit took. A file named by a segment whose rows may
    /// have been committed is kept, so a file is deleted only when nothing could reference it;
    /// a file kept wrongly is the orphan sweep's.
    fn uncommitted_files(self, segments: &[(ListedSegment, SegmentFiles)]) -> BTreeSet<String> {
        let maybe_committed = |segment: &ListedSegment| match self {
            CommittedRows::Nothing => false,
            CommittedRows::Through(flushed) => segment.last <= flushed,
            CommittedRows::Everything | CommittedRows::Unknown => true,
        };
        let paths = |committed: bool| -> BTreeSet<String> {
            segments
                .iter()
                .filter(|(segment, _)| maybe_committed(segment) == committed)
                .flat_map(|(_, staged)| staged.files.iter().map(|file| file.path.clone()))
                .collect()
        };
        &paths(false) - &paths(true)
    }
}

#[cfg(test)]
mod tests;
