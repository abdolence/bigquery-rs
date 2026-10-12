//! The write streams `CreateWriteStream` makes. Only a stream's creator serves its appends,
//! flushes and finalize, since only it knows the next offset; a batch commit and
//! `GetWriteStream` read storage and the table, so any engine on the root serves them.
//!
//! A pending or buffered stream's state lives in its table's properties, and every commit of
//! its rows changes that state only from the state it expects. The pointer chain therefore
//! fences each engine's commits of the stream against every other's, and a sweep's drop lands
//! before anything of the stream is deleted.
//!
//! A stream's lock is taken before the table's commit lock. A commit whose outcome is unknown
//! stops the stream: whatever comes next is refused rather than risk a second copy.

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

pub(crate) struct Streams {
    catalog: Arc<EngineCatalog>,
    groups: Arc<CommitGroups>,
    staging: Staging,
    ttl: Duration,
    node: NodeId,
    created: Mutex<HashMap<WriteStreamId, Arc<tokio::sync::Mutex<Stream>>>>,
    /// When the last sweep started; held while a sweep runs.
    sweeping: Arc<tokio::sync::Mutex<Option<Instant>>>,
}

impl std::fmt::Debug for Streams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Streams")
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

struct Stream {
    id: WriteStreamId,
    record: StreamRecord,
    /// No later than the backend's creation time of the record, which a sweep measures the
    /// TTL from.
    written: Instant,
    next: RowOffset,
    finalized: bool,
    flushed: Option<RowOffset>,
    /// A commit of the stream's rows ended with an unknown outcome.
    failed: bool,
}

pub(crate) struct StreamDescription {
    pub kind: Type,
    pub created_at_ms: i64,
    pub committed_at_ms: Option<i64>,
}

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

    /// A pending stream is finalized in its table, where a batch commit on any engine finds
    /// it; the others in their creator only.
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

    /// The segment with rows on both sides of `offset` is written again as two segments
    /// before the commit, which takes the lower one; the original goes only once the commit
    /// has landed.
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

    /// A sweep drops a stream whose record the backend created a TTL ago, so rewriting the
    /// record after a commit makes a committed or buffered stream live the TTL from its last
    /// commit; a pending stream lives it from its creation.
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

    /// Returns the lower segment's files and the paths of the rewritten files, which no segment
    /// names once the original is gone.
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

    /// Commits every named stream or none: one that is not a finalized, uncommitted pending
    /// stream of `table` goes to `stream_errors`, and nothing is committed.
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

    fn refused(refused: &[EngineError]) -> storage::BatchCommitWriteStreamsResponse {
        storage::BatchCommitWriteStreamsResponse {
            commit_time: None,
            stream_errors: refused
                .iter()
                .filter_map(EngineError::stream_error)
                .collect(),
        }
    }

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

    /// A segment listed and then gone was deleted by a sweep that dropped its stream.
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

    /// No request waits for a sweep, and a busy engine lists the root's streams only a few
    /// times per TTL.
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

    #[cfg(test)]
    pub(crate) async fn sweep(&self) -> Result<(), EngineError> {
        let _running = self.sweeping.lock().await;
        self.sweep_expired().await
    }

    /// An expired stream's state leaves its table before any of its files is deleted, so that
    /// no later commit can take them.
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
            .commit(&record.table, |loaded: &LoadedTable| {
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
    Through(RowOffset),
    Everything,
    /// The table is gone, is another identity, or another sweep removed the state first.
    Unknown,
}

impl CommittedRows {
    /// A file named by any segment whose rows may have been committed is kept; a file kept
    /// wrongly is the orphan sweep's.
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
