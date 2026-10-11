//! The write streams `CreateWriteStream` makes, each with offsets, so that a request sent again
//! lands once:
//!
//! - a committed stream commits each request as it comes, through the table's commit group;
//! - a pending stream stages each request as a segment and commits every segment at
//!   `BatchCommitWriteStreams`, together with the other streams named there, in one commit;
//! - a buffered stream stages each request as a segment and commits the segments up to the
//!   offset of each `FlushRows`.
//!
//! What a pending or buffered stream staged is on storage before a request is answered, and
//! what it committed is in the table properties that the commit adding its files sets. So the
//! stream outlives the process that created it, and any engine on the root rebuilds it when it
//! is first asked for it. A committed stream's next offset lives in the process that created
//! it, and any other process answers `STREAM_NOT_FOUND` for it.
//!
//! Each stream has a lock, held across every change of its state and taken before the table's
//! commit lock. A commit whose outcome is unknown stops the stream in this process: whatever
//! comes next is refused rather than risk a second copy. A process that rebuilds the stream
//! finds the outcome in the table.

use crate::catalog::{now_ms, Change, EngineCatalog, EngineCommit, LoadedTable};
use crate::error::{EngineError, Resource, StreamProblem};
use crate::ids::{RowOffset, StreamId, TableRef, WriteStreamId, WriteStreamName};
use crate::storage::{
    select_segments, ListedSegment, PendingState, SegmentFiles, StagedFile, Staging, StreamChange,
    StreamMode, StreamProperty, StreamRecord,
};
use crate::write::{proto_time, CommitGroups, TableWriter, WriteDisposition, WrittenFiles};
use arrow::array::RecordBatch;
use gcloud_sdk::google::cloud::bigquery::storage::v1 as storage;
use iceberg::TableUpdate;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime};
use storage::write_stream::Type;
use tokio::sync::{OwnedMappedMutexGuard, OwnedMutexGuard};
use tracing::{info, warn};
use uuid::Uuid;

/// The created write streams of one engine.
pub(crate) struct Streams {
    catalog: Arc<EngineCatalog>,
    groups: Arc<CommitGroups>,
    staging: Staging,
    ttl: Duration,
    slots: Mutex<HashMap<WriteStreamId, Arc<tokio::sync::Mutex<Slot>>>>,
}

impl std::fmt::Debug for Streams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Streams")
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

/// What this process knows of a stream ID.
enum Slot {
    /// Not looked up yet: the stream, if any, is on storage.
    Unread,
    Live(Stream),
    /// No such stream, or one this process cannot serve.
    Gone,
}

struct Stream {
    id: WriteStreamId,
    table: TableRef,
    /// The table identity the stream writes into.
    table_uuid: Uuid,
    created_at_ms: i64,
    /// The offset the next append takes.
    next: RowOffset,
    /// Set once a commit of the stream's rows ended with an unknown outcome.
    failed: bool,
    mode: ModeState,
}

/// What a stream of each type keeps besides its offsets.
enum ModeState {
    Committed {
        finalized: bool,
    },
    Pending {
        state: PendingState,
        committed_at_ms: Option<i64>,
    },
    /// `flushed` is the offset of the last row a flush committed.
    Buffered {
        finalized: bool,
        flushed: Option<RowOffset>,
    },
}

/// A stream, locked.
type Locked = OwnedMappedMutexGuard<Slot, Stream>;

/// What `GetWriteStream` reports of a created stream.
pub(crate) struct StreamDescription {
    pub kind: Type,
    pub created_at_ms: i64,
    /// When a pending stream's rows were committed, as far as this process knows.
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

impl From<StreamMode> for ModeState {
    /// A new stream of the type.
    fn from(mode: StreamMode) -> Self {
        match mode {
            StreamMode::Committed => ModeState::Committed { finalized: false },
            StreamMode::Pending => ModeState::Pending {
                state: PendingState::Open,
                committed_at_ms: None,
            },
            StreamMode::Buffered => ModeState::Buffered {
                finalized: false,
                flushed: None,
            },
        }
    }
}

impl Stream {
    fn name(&self) -> WriteStreamName {
        WriteStreamName {
            table: self.table.clone(),
            stream: StreamId::Created(self.id),
        }
    }

    fn finalized(&self) -> bool {
        match self.mode {
            ModeState::Committed { finalized } | ModeState::Buffered { finalized, .. } => finalized,
            ModeState::Pending { state, .. } => state != PendingState::Open,
        }
    }

    /// Refuses an append at `offset` the stream cannot take.
    fn check_append(&self, offset: RowOffset) -> Result<(), EngineError> {
        let problem = if self.failed {
            StreamProblem::OutcomeUnknown
        } else if self.finalized() {
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

    fn description(&self) -> StreamDescription {
        let (kind, committed_at_ms) = match self.mode {
            ModeState::Committed { .. } => (Type::Committed, None),
            ModeState::Pending {
                committed_at_ms, ..
            } => (Type::Pending, committed_at_ms),
            ModeState::Buffered { .. } => (Type::Buffered, None),
        };
        StreamDescription {
            kind,
            created_at_ms: self.created_at_ms,
            committed_at_ms,
        }
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
            slots: Mutex::default(),
        }
    }

    /// Creates a stream of `kind` on `table`, whose record is on storage when this returns.
    /// The root's expired streams are dropped on the way.
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
        };
        self.staging.write_record(id, &record).await?;
        let stream = Stream {
            id,
            table: record.table,
            table_uuid: record.table_uuid,
            created_at_ms: record.created_at_ms,
            next: RowOffset::default(),
            failed: false,
            mode: ModeState::from(mode),
        };
        let name = stream.name();
        self.slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                stream.id,
                Arc::new(tokio::sync::Mutex::new(Slot::Live(stream))),
            );
        if let Err(err) = self.sweep().await {
            warn!(%err, "The expired write streams could not all be dropped.");
        }
        Ok(name)
    }

    /// What `GetWriteStream` reports of the stream.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] for a stream that does not exist.
    pub(crate) async fn describe(
        &self,
        name: &WriteStreamName,
    ) -> Result<StreamDescription, EngineError> {
        Ok(self.lock(name, "GetWriteStream").await?.description())
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
        stream.check_identity(loaded)?;
        let rows = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
        if rows == 0 {
            return Ok(offset);
        }
        let writer = TableWriter::new(loaded)?;
        if let ModeState::Committed { .. } = stream.mode {
            let files = writer.write([batch]).await?;
            let landed = self
                .groups
                .append(&stream.table, files, WriteDisposition::Append)
                .await;
            if let Err(err) = landed {
                stream.failed |= err.commit_may_have_landed();
                return Err(err);
            }
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
    /// last row. A pending stream is finalized in its table's properties, where a batch commit
    /// on any process finds it; the others in this process only. Finalizing a finalized stream
    /// changes nothing.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] for a stream that does not exist or whose table is not the
    /// identity it writes into, and the errors of the pending stream's commit.
    pub(crate) async fn finalize(&self, name: &WriteStreamName) -> Result<RowOffset, EngineError> {
        let mut stream = self.lock(name, "FinalizeWriteStream").await?;
        let open_pending = match &mut stream.mode {
            ModeState::Committed { finalized } | ModeState::Buffered { finalized, .. } => {
                *finalized = true;
                false
            }
            ModeState::Pending { state, .. } => *state == PendingState::Open,
        };
        if open_pending {
            if stream.failed {
                return Err(EngineError::stream(name, StreamProblem::OutcomeUnknown));
            }
            match self.finalize_pending(&stream).await {
                Ok(found) => {
                    stream.mode = ModeState::Pending {
                        state: match found {
                            PendingState::Open => PendingState::Finalized,
                            PendingState::Finalized | PendingState::Committed => found,
                        },
                        committed_at_ms: None,
                    };
                }
                Err(err) => {
                    stream.failed |= err.commit_may_have_landed();
                    return Err(err);
                }
            }
        }
        Ok(stream.next)
    }

    /// Sets the pending stream's state property to finalized while the table holds it open,
    /// and returns the state the table held.
    async fn finalize_pending(&self, stream: &Stream) -> Result<PendingState, EngineError> {
        let mut found = PendingState::Open;
        self.catalog
            .commit(&stream.table, |loaded| {
                stream.check_identity(loaded)?;
                found = PendingState::in_table(stream.id, loaded.metadata().properties())?;
                let updates = match found {
                    PendingState::Open => vec![TableUpdate::SetProperties {
                        updates: HashMap::from([(
                            StreamProperty::State.key(stream.id),
                            PendingState::Finalized.as_str().to_string(),
                        )]),
                    }],
                    PendingState::Finalized | PendingState::Committed => Vec::new(),
                };
                Ok(Change::Engine(EngineCommit {
                    requirements: Vec::new(),
                    updates,
                }))
            })
            .await?;
        Ok(found)
    }

    /// Commits every row of the buffered stream up to and including `offset`, and returns
    /// `offset`. A flush at an offset already flushed commits nothing.
    ///
    /// The segment with rows on both sides of `offset` is first written again as two segments,
    /// one per side, both before the commit; the commit then takes the lower one, and the
    /// original goes once the commit has landed.
    ///
    /// # Errors
    /// [`EngineError::WriteStream`] for a stream that is not buffered or whose last commit's
    /// outcome is unknown, [`EngineError::OutOfRange`] for an offset past the stream's rows,
    /// and the errors of the split and the commit.
    pub(crate) async fn flush(
        &self,
        name: &WriteStreamName,
        offset: RowOffset,
    ) -> Result<RowOffset, EngineError> {
        let mut stream = self.lock(name, "FlushRows").await?;
        let ModeState::Buffered { flushed, .. } = stream.mode else {
            return Err(EngineError::stream(
                name,
                StreamProblem::WrongType {
                    request: "FlushRows",
                    applies_to: "BUFFERED streams",
                },
            ));
        };
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
        if flushed.is_some_and(|flushed| offset <= flushed) {
            return Ok(offset);
        }
        let loaded = self.catalog.load(&stream.table).await?;
        stream.check_identity(&loaded)?;

        let mut committing = Vec::new();
        let mut straddling = None;
        for segment in self.select(&stream, flushed).await? {
            if segment.first > offset {
                break;
            }
            let staged = self.staging.read_segment(stream.id, &segment).await?;
            if segment.last <= offset {
                committing.extend(staged.files);
            } else {
                let (lower, rewritten) = self
                    .split(&stream, &loaded, &segment, staged, offset)
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
                &stream.table,
                WrittenFiles::staged(&committing, &loaded)?,
                vec![change],
            )
            .await;
        if let Err(err) = landed {
            stream.failed |= err.commit_may_have_landed();
            return Err(err);
        }
        if let ModeState::Buffered { flushed, .. } = &mut stream.mode {
            *flushed = Some(offset);
        }
        if let Some((segment, rewritten)) = straddling {
            self.staging.delete_segment(stream.id, &segment).await;
            for path in rewritten {
                self.staging.delete_data_file(&path).await;
            }
        }
        Ok(offset)
    }

    /// Writes `segment`, whose rows straddle `offset`, again as two segments: its rows up to
    /// and including `offset`, and the rest. A file with rows on both sides is rewritten as
    /// two; any other goes to its side as it is. Returns the files of the lower segment and the
    /// paths of the files rewritten, which no segment names once the original is gone.
    async fn split(
        &self,
        stream: &Stream,
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
            .write_segment(stream.id, segment.first, offset, &lower)
            .await?;
        self.staging
            .write_segment(stream.id, end, segment.last, &SegmentFiles { files: upper })
            .await?;
        Ok((lower.files, rewritten))
    }

    /// Commits every named pending stream of `table` in one commit, or none of them: a stream
    /// that is not a finalized, uncommitted pending stream of the table is reported in the
    /// response's `stream_errors`, and then nothing is committed.
    ///
    /// # Errors
    /// [`EngineError::Invalid`] for a name that is no write stream's, and the errors of the
    /// commit, after which a stream whose commit's outcome is unknown takes no other commit.
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
        // One order for every call, so that two calls naming the same streams never wait on
        // each other.
        named.sort_by_key(ToString::to_string);
        named.dedup();
        let mut locked = Vec::new();
        for name in &named {
            match self.lock(name, "BatchCommitWriteStreams").await {
                Ok(stream) => locked.push(stream),
                Err(err @ (EngineError::NotFound { .. } | EngineError::WriteStream { .. })) => {
                    refused.push(err);
                }
                Err(err) => return Err(err),
            }
        }
        for stream in &locked {
            let problem = match stream.mode {
                ModeState::Pending { .. } if stream.failed => Some(StreamProblem::OutcomeUnknown),
                ModeState::Pending { state, .. } => match state {
                    PendingState::Finalized => None,
                    PendingState::Committed => Some(StreamProblem::AlreadyCommitted),
                    PendingState::Open => Some(StreamProblem::NotFinalized),
                },
                ModeState::Committed { .. } | ModeState::Buffered { .. } => {
                    Some(StreamProblem::WrongType {
                        request: "BatchCommitWriteStreams",
                        applies_to: "PENDING streams",
                    })
                }
            };
            refused.extend(problem.map(|problem| EngineError::stream(&stream.name(), problem)));
        }
        if !refused.is_empty() {
            return Ok(Self::refused(&refused));
        }

        let loaded = self.catalog.load(table).await?;
        let mut staged = Vec::new();
        for stream in &locked {
            stream.check_identity(&loaded)?;
            for segment in self.select(stream, None).await? {
                staged.extend(self.staging.read_segment(stream.id, &segment).await?.files);
            }
        }
        let files = WrittenFiles::staged(&staged, &loaded)?;
        let changes = locked
            .iter()
            .map(|stream| StreamChange::Commit(stream.id))
            .collect();
        let landed = self.groups.commit_streams(table, files, changes).await;
        let committed_at_ms = now_ms();
        for stream in &mut locked {
            match &landed {
                Ok(()) => {
                    stream.mode = ModeState::Pending {
                        state: PendingState::Committed,
                        committed_at_ms: Some(committed_at_ms),
                    };
                }
                Err(err) => stream.failed |= err.commit_may_have_landed(),
            }
        }
        match landed {
            Ok(()) => Ok(storage::BatchCommitWriteStreamsResponse {
                commit_time: proto_time(committed_at_ms),
                stream_errors: Vec::new(),
            }),
            Err(err @ EngineError::WriteStream { .. }) => Ok(Self::refused(&[err])),
            Err(err) => Err(err),
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
    /// [`EngineError::WriteStream`] when the segments do not line up, which only two processes
    /// appending to one stream produce, and the storage errors of the listing.
    async fn select(
        &self,
        stream: &Stream,
        flushed: Option<RowOffset>,
    ) -> Result<Vec<ListedSegment>, EngineError> {
        select_segments(self.staging.segments(stream.id).await?, flushed).map_err(|segment| {
            warn!(stream = %stream.id, segment = segment.name, "A write stream's segments overlap.");
            EngineError::stream(&stream.name(), StreamProblem::ChangedElsewhere)
        })
    }

    /// Drops every stream whose record the backend created more than the stream TTL ago, unless
    /// this process is using it right now. Its committed state leaves the table's properties
    /// first, so that no later commit can take its files; then the files no commit took, its
    /// segments and its record are deleted.
    ///
    /// # Errors
    /// The storage errors of listing the records; a stream that cannot be dropped is logged
    /// and left for the next sweep.
    pub(crate) async fn sweep(&self) -> Result<(), EngineError> {
        let now = SystemTime::now();
        for (id, created) in self.staging.records().await? {
            if now
                .duration_since(created)
                .map_or(true, |age| age < self.ttl)
            {
                continue;
            }
            let Ok(mut slot) = self.slot(id).try_lock_owned() else {
                continue;
            };
            match self.expire(id).await {
                Ok(()) => {
                    *slot = Slot::Gone;
                    info!(stream = %id, "Dropped a write stream past its TTL.");
                }
                Err(err) => {
                    warn!(stream = %id, %err, "An expired write stream could not be dropped.")
                }
            }
        }
        self.slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|_, slot| {
                !slot
                    .try_lock()
                    .is_ok_and(|slot| matches!(*slot, Slot::Gone))
            });
        Ok(())
    }

    /// Deletes the stream `id` from storage, its committed state first.
    async fn expire(&self, id: WriteStreamId) -> Result<(), EngineError> {
        if let Some(record) = self.staging.read_record(id).await? {
            let committed = self.release(id, &record).await?;
            let mut segments = Vec::new();
            for segment in self.staging.segments(id).await? {
                let files = self.staging.read_segment(id, &segment).await?;
                segments.push((segment, files));
            }
            for path in committed.uncommitted_files(&segments) {
                self.staging.delete_data_file(&path).await;
            }
        }
        self.staging.delete_stream(id).await
    }

    /// Removes the stream's property from its table, and returns what it said the stream had
    /// committed. A table that is gone, or is another identity, holds no property of the
    /// stream, and what the stream committed is not known.
    async fn release(
        &self,
        id: WriteStreamId,
        record: &StreamRecord,
    ) -> Result<CommittedRows, EngineError> {
        let property = match record.mode {
            // Its requests commit as they come and stage nothing.
            StreamMode::Committed => return Ok(CommittedRows::Everything),
            StreamMode::Pending => StreamProperty::State,
            StreamMode::Buffered => StreamProperty::Flushed,
        };
        let key = property.key(id);
        let mut committed = CommittedRows::Unknown;
        let released = self
            .catalog
            .commit(&record.table, |loaded| {
                if loaded.metadata().uuid() != record.table_uuid {
                    committed = CommittedRows::Unknown;
                    return Ok(Change::Engine(EngineCommit::default()));
                }
                let value = loaded.metadata().properties().get(&key);
                committed = match property {
                    StreamProperty::State => {
                        match PendingState::from_property(value.map(String::as_str))? {
                            PendingState::Committed => CommittedRows::Everything,
                            PendingState::Open | PendingState::Finalized => CommittedRows::Nothing,
                        }
                    }
                    StreamProperty::Flushed => match value {
                        Some(flushed) => CommittedRows::Through(flushed.parse()?),
                        None => CommittedRows::Nothing,
                    },
                };
                Ok(Change::Engine(EngineCommit {
                    requirements: Vec::new(),
                    updates: value
                        .map(|_| TableUpdate::RemoveProperties {
                            removals: vec![key.clone()],
                        })
                        .into_iter()
                        .collect(),
                }))
            })
            .await;
        match released {
            Ok(_) => Ok(committed),
            Err(EngineError::NotFound { .. }) => Ok(CommittedRows::Unknown),
            Err(err) => Err(err),
        }
    }

    /// The created stream `name` names, locked, for `request`.
    async fn lock(
        &self,
        name: &WriteStreamName,
        request: &'static str,
    ) -> Result<Locked, EngineError> {
        let StreamId::Created(id) = name.stream else {
            return Err(EngineError::stream(
                name,
                StreamProblem::WrongType {
                    request,
                    applies_to: "created streams",
                },
            ));
        };
        let mut slot = self.slot(id).lock_owned().await;
        if matches!(*slot, Slot::Unread) {
            *slot = match self.rebuild(id).await? {
                Some(stream) => Slot::Live(stream),
                None => Slot::Gone,
            };
        }
        OwnedMutexGuard::try_map(slot, |slot| match slot {
            Slot::Live(stream) if stream.table == name.table => Some(stream),
            _ => None,
        })
        .map_err(|_| EngineError::stream_not_found(name))
    }

    fn slot(&self, id: WriteStreamId) -> Arc<tokio::sync::Mutex<Slot>> {
        Arc::clone(
            self.slots
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(id)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(Slot::Unread))),
        )
    }

    /// The pending or buffered stream `id` as storage holds it: its record, its segments, and
    /// the state its table's properties hold. `None` when it has no record, and for a
    /// committed stream, whose next offset only the process that created it knows.
    async fn rebuild(&self, id: WriteStreamId) -> Result<Option<Stream>, EngineError> {
        let Some(record) = self.staging.read_record(id).await? else {
            return Ok(None);
        };
        let mode = match record.mode {
            StreamMode::Committed => return Ok(None),
            StreamMode::Pending => ModeState::Pending {
                state: PendingState::in_table(id, &self.properties(&record).await?)?,
                committed_at_ms: None,
            },
            StreamMode::Buffered => ModeState::Buffered {
                finalized: false,
                flushed: self
                    .properties(&record)
                    .await?
                    .get(&StreamProperty::Flushed.key(id))
                    .map(|flushed| flushed.parse())
                    .transpose()?,
            },
        };
        let flushed = match mode {
            ModeState::Buffered { flushed, .. } => flushed,
            ModeState::Committed { .. } | ModeState::Pending { .. } => None,
        };
        let next = self
            .staging
            .segments(id)
            .await?
            .iter()
            .map(|segment| segment.last)
            .chain(flushed)
            .max()
            .map_or(RowOffset::default(), |last| last.after(1));
        Ok(Some(Stream {
            id,
            table: record.table,
            table_uuid: record.table_uuid,
            created_at_ms: record.created_at_ms,
            next,
            failed: false,
            mode,
        }))
    }

    /// The properties of the stream's table, none while the table is gone or is another
    /// identity than the one the stream writes into.
    async fn properties(
        &self,
        record: &StreamRecord,
    ) -> Result<HashMap<String, String>, EngineError> {
        match self.catalog.load(&record.table).await {
            Ok(loaded) if loaded.metadata().uuid() == record.table_uuid => {
                Ok(loaded.metadata().properties().clone())
            }
            Ok(_) | Err(EngineError::NotFound { .. }) => Ok(HashMap::new()),
            Err(err) => Err(err),
        }
    }
}

/// What an expired stream committed, as its table's property said when it was removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommittedRows {
    Nothing,
    /// The rows up to and including the offset.
    Through(RowOffset),
    Everything,
    /// The table is gone or is another identity.
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
