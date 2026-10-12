//! What the root holds of the write streams `CreateWriteStream` makes:
//!
//! - the stream record, `_smallquery/streams/<id>.json`, written before the stream is
//!   reported: its type, its table and the engine that created it;
//! - the segments of a pending or buffered stream,
//!   `_smallquery/streams/<id>/seg-<first>-<last>-<uuid>.json`: one acknowledged request each,
//!   offsets `first` to `last`, with the data files it staged. A segment is written before its
//!   request is acknowledged and never rewritten;
//! - the table properties `smallquery.stream.<id>.state` and `smallquery.stream.<id>.flushed`
//!   of a pending or buffered stream, from the commit that registers the stream to the one that
//!   drops it. Every commit of the stream's rows changes them, and only from the state it
//!   expects, so the table's pointer chain fences each engine's commits against every other's.
//!
//! A staged data file lives in the table's `data/` directory under its final name, and no
//! snapshot references it until a flush or a batch commit adds it. Its rows' offsets are
//! recorded with it, so that a flush can tell which of its rows are at or below the flushed
//! offset.

use super::{Backend, ListedKind, ObjectPath};
use crate::error::{EngineError, StreamProblem};
use crate::ids::{NodeId, RowOffset, TableRef, WriteStreamId};
use bytes::Bytes;
use iceberg::spec::{
    deserialize_data_file_from_json, serialize_data_file_to_json, DataFile, TableMetadata,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;
use tracing::warn;
use uuid::Uuid;

/// A stream's record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StreamRecord {
    pub mode: StreamMode,
    pub table: TableRef,
    /// The table identity the stream writes into: a table dropped and created again under the
    /// name is another table, which the stream's rows never reach.
    pub table_uuid: Uuid,
    pub created_at_ms: i64,
    pub creator: NodeId,
}

/// The type of a created stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StreamMode {
    Committed,
    Pending,
    Buffered,
}

/// The table properties a pending or buffered stream's state is kept in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamProperty {
    State,
    Flushed,
}

impl StreamProperty {
    fn key(self, stream: WriteStreamId) -> String {
        let name = match self {
            StreamProperty::State => "state",
            StreamProperty::Flushed => "flushed",
        };
        format!("smallquery.stream.{stream}.{name}")
    }
}

/// Where a pending or buffered stream is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamState {
    Open,
    /// A pending stream that takes no more rows; a buffered stream's finalize stays with its
    /// creator.
    Finalized,
    Committed,
}

impl StreamState {
    fn as_str(self) -> &'static str {
        match self {
            StreamState::Open => "open",
            StreamState::Finalized => "finalized",
            StreamState::Committed => "committed",
        }
    }
}

/// # Errors
/// [`EngineError::Internal`] for a value the engine never writes.
impl std::str::FromStr for StreamState {
    type Err = EngineError;

    fn from_str(state: &str) -> Result<Self, Self::Err> {
        match state {
            "open" => Ok(StreamState::Open),
            "finalized" => Ok(StreamState::Finalized),
            "committed" => Ok(StreamState::Committed),
            other => Err(EngineError::Internal {
                detail: format!("{other:?} is no write stream state"),
            }),
        }
    }
}

/// What a table holds of a pending or buffered stream, from the commit that registers it at its
/// creation to the one that drops it: a stream whose state is absent is gone, and no commit
/// takes its rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StoredStream {
    pub state: StreamState,
    /// A buffered stream's last flushed offset.
    pub flushed: Option<RowOffset>,
}

impl StoredStream {
    /// What `properties` hold of `stream`.
    ///
    /// # Errors
    /// [`EngineError::Internal`] for a value the engine never writes.
    pub(crate) fn read(
        stream: WriteStreamId,
        properties: &HashMap<String, String>,
    ) -> Result<Option<Self>, EngineError> {
        let value = |property: StreamProperty| properties.get(&property.key(stream));
        let Some(state) = value(StreamProperty::State) else {
            return Ok(None);
        };
        Ok(Some(Self {
            state: state.parse()?,
            flushed: value(StreamProperty::Flushed)
                .map(|flushed| flushed.parse())
                .transpose()?,
        }))
    }

    /// The properties that hold this state of `stream`.
    pub(crate) fn properties(self, stream: WriteStreamId) -> Vec<(String, String)> {
        let mut properties = vec![(
            StreamProperty::State.key(stream),
            self.state.as_str().to_string(),
        )];
        properties.extend(
            self.flushed
                .map(|flushed| (StreamProperty::Flushed.key(stream), flushed.to_string())),
        );
        properties
    }

    /// The keys of every property a table may hold of `stream`.
    pub(crate) fn keys(stream: WriteStreamId) -> [String; 2] {
        [
            StreamProperty::State.key(stream),
            StreamProperty::Flushed.key(stream),
        ]
    }
}

/// A change of a stream's state, made by a commit only on the state it starts from: whatever
/// engine commits it, the table then holds exactly what the stream has committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamChange {
    /// A new pending or buffered stream: until it is registered, and once it is dropped, no
    /// commit takes its rows.
    Register(WriteStreamId),
    /// A pending stream takes no more rows.
    Finalize(WriteStreamId),
    /// A batch commit: the pending stream goes from finalized to committed.
    Commit(WriteStreamId),
    /// A flush: the buffered stream's last flushed offset goes from `from`, `None` before its
    /// first flush, to `to`.
    Flush {
        stream: WriteStreamId,
        from: Option<RowOffset>,
        to: RowOffset,
    },
}

impl StreamChange {
    pub(crate) fn stream(self) -> WriteStreamId {
        match self {
            StreamChange::Register(stream)
            | StreamChange::Finalize(stream)
            | StreamChange::Commit(stream)
            | StreamChange::Flush { stream, .. } => stream,
        }
    }

    /// The stream's state after the change, given the state `current` it finds.
    ///
    /// # Errors
    /// Why the stream refuses the change when `current` is not the state it starts from.
    pub(crate) fn apply(
        self,
        current: Option<StoredStream>,
    ) -> Result<StoredStream, StreamProblem> {
        let Some(current) = current else {
            return match self {
                StreamChange::Register(_) => Ok(StoredStream {
                    state: StreamState::Open,
                    flushed: None,
                }),
                _ => Err(StreamProblem::Expired),
            };
        };
        match (self, current.state) {
            (StreamChange::Finalize(_), StreamState::Open) => Ok(StoredStream {
                state: StreamState::Finalized,
                ..current
            }),
            (StreamChange::Commit(_), StreamState::Finalized) => Ok(StoredStream {
                state: StreamState::Committed,
                ..current
            }),
            (StreamChange::Commit(_), StreamState::Committed) => {
                Err(StreamProblem::AlreadyCommitted)
            }
            (StreamChange::Commit(_), StreamState::Open) => Err(StreamProblem::NotFinalized),
            (StreamChange::Flush { from, to, .. }, StreamState::Open)
                if current.flushed == from =>
            {
                Ok(StoredStream {
                    flushed: Some(to),
                    ..current
                })
            }
            _ => Err(StreamProblem::ChangedElsewhere),
        }
    }
}

/// A segment as a listing names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ListedSegment {
    pub first: RowOffset,
    /// The offset of the segment's last row.
    pub last: RowOffset,
    /// The object's name.
    pub name: String,
    pub created: SystemTime,
}

impl ListedSegment {
    /// The segment an object named `name` holds, for names [`Staging::write_segment`] gives;
    /// anything else in the directory is no segment.
    fn parse(name: &str, created: SystemTime) -> Option<Self> {
        let rest = name.strip_prefix("seg-")?.strip_suffix(".json")?;
        let mut parts = rest.splitn(3, '-');
        Some(Self {
            first: parts.next()?.parse().ok()?,
            last: parts.next()?.parse().ok()?,
            name: name.to_string(),
            created,
        })
    }
}

/// The segments a commit takes of everything a stream staged, in offset order: none at or
/// below `flushed`, whose rows a flush committed through the lower half of a split, and one
/// per range, since a request sent again, or the halves of a split whose original survived,
/// stage the same rows twice. Of segments sharing a first offset the longest is taken, then
/// the one the backend created first.
///
/// # Errors
/// The segment that leaves a gap after the ones taken, or overlaps one without being covered
/// by it: no order of the engine's writes produces either.
pub(crate) fn select_segments(
    mut segments: Vec<ListedSegment>,
    flushed: Option<RowOffset>,
) -> Result<Vec<ListedSegment>, ListedSegment> {
    segments.retain(|segment| flushed.is_none_or(|flushed| segment.first > flushed));
    segments.sort_by(|a, b| {
        a.first
            .cmp(&b.first)
            .then(b.last.cmp(&a.last))
            .then(a.created.cmp(&b.created))
    });
    let mut next = flushed.map_or(RowOffset::default(), |flushed| flushed.after(1));
    let mut taken = Vec::new();
    for segment in segments {
        if segment.last < next {
            continue;
        }
        if segment.first != next {
            return Err(segment);
        }
        next = segment.last.after(1);
        taken.push(segment);
    }
    Ok(taken)
}

/// What a segment holds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SegmentFiles {
    pub files: Vec<StagedFile>,
}

/// A data file of staged rows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StagedFile {
    /// The file's Iceberg location.
    pub path: String,
    /// The offsets of the file's rows, in the file's row order.
    pub rows: OffsetRanges,
    /// The schema the file was written with.
    pub schema_id: i32,
    /// The Iceberg `DataFile`, in Iceberg's own JSON form.
    pub data_file: serde_json::Value,
}

impl StagedFile {
    /// `file`, written with `metadata`'s current schema and default partition spec, whose rows
    /// have the offsets `rows`.
    pub(crate) fn new(
        file: DataFile,
        rows: OffsetRanges,
        metadata: &TableMetadata,
    ) -> Result<Self, EngineError> {
        let schema = metadata.current_schema();
        let partition_type = metadata.default_partition_spec().partition_type(schema)?;
        let path = file.file_path().to_string();
        let json = serialize_data_file_to_json(file, &partition_type, metadata.format_version())?;
        Ok(Self {
            path,
            rows,
            schema_id: schema.schema_id(),
            data_file: serde_json::from_str(&json).map_err(|err| EngineError::Internal {
                detail: format!("a data file's JSON does not parse: {err}"),
            })?,
        })
    }

    /// The Iceberg `DataFile`, read with the schema it was written with from `metadata`, the
    /// metadata of the table identity it was written for.
    pub(crate) fn data_file(&self, metadata: &TableMetadata) -> Result<DataFile, EngineError> {
        let schema =
            metadata
                .schema_by_id(self.schema_id)
                .ok_or_else(|| EngineError::Internal {
                    detail: format!(
                        "{} was written with schema {}, which the table no longer has",
                        self.path, self.schema_id
                    ),
                })?;
        let spec = metadata.default_partition_spec();
        Ok(deserialize_data_file_from_json(
            &self.data_file.to_string(),
            spec.spec_id(),
            &spec.partition_type(schema)?,
            schema,
        )?)
    }
}

/// The offsets of a run of consecutive rows: `start` up to `end`, without `end`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OffsetRange {
    pub start: RowOffset,
    pub end: RowOffset,
}

/// Offsets in ascending order, as runs of consecutive ones. A file of an unpartitioned table
/// holds one run; a file of one partition holds a run for each stretch of a request's rows
/// that fell into that partition.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct OffsetRanges(Vec<OffsetRange>);

impl OffsetRanges {
    /// `offsets`, which ascend.
    pub(crate) fn from_ascending(offsets: impl IntoIterator<Item = RowOffset>) -> Self {
        let mut ranges: Vec<OffsetRange> = Vec::new();
        for offset in offsets {
            match ranges.last_mut() {
                Some(last) if last.end == offset => last.end = offset.after(1),
                _ => ranges.push(OffsetRange {
                    start: offset,
                    end: offset.after(1),
                }),
            }
        }
        Self(ranges)
    }

    /// The number of offsets.
    pub(crate) fn rows(&self) -> u64 {
        self.0
            .iter()
            .map(|range| range.start.rows_to(range.end))
            .sum()
    }

    /// The number of offsets below `end`; they come first.
    pub(crate) fn rows_before(&self, end: RowOffset) -> u64 {
        self.0
            .iter()
            .map(|range| range.start.rows_to(range.end.min(end)))
            .sum()
    }

    /// The first `rows` offsets, and the rest.
    pub(crate) fn split(self, rows: u64) -> (Self, Self) {
        let mut head = Vec::new();
        let mut tail = Vec::new();
        let mut left = rows;
        for range in self.0 {
            let length = range.start.rows_to(range.end);
            if left >= length {
                head.push(range);
                left -= length;
            } else if left == 0 {
                tail.push(range);
            } else {
                let cut = range.start.after(left);
                head.push(OffsetRange {
                    start: range.start,
                    end: cut,
                });
                tail.push(OffsetRange {
                    start: cut,
                    end: range.end,
                });
                left = 0;
            }
        }
        (Self(head), Self(tail))
    }
}

/// The write streams of one root on its storage.
#[derive(Debug, Clone)]
pub(crate) struct Staging {
    backend: Backend,
}

impl Staging {
    pub(crate) fn new(backend: Backend) -> Self {
        Self { backend }
    }

    fn directory() -> ObjectPath {
        ObjectPath::segment("_smallquery").join("streams")
    }

    fn record(stream: WriteStreamId) -> ObjectPath {
        Self::directory().join(format_args!("{stream}.json"))
    }

    fn segments_directory(stream: WriteStreamId) -> ObjectPath {
        Self::directory().join(stream)
    }

    /// Writes the stream's record; durable once this returns.
    pub(crate) async fn write_record(
        &self,
        stream: WriteStreamId,
        record: &StreamRecord,
    ) -> Result<(), EngineError> {
        self.put(&Self::record(stream), record).await
    }

    /// The stream's record, or `None` when the stream has none.
    pub(crate) async fn read_record(
        &self,
        stream: WriteStreamId,
    ) -> Result<Option<StreamRecord>, EngineError> {
        self.get(&Self::record(stream)).await
    }

    /// Every stream with a record, with the time the backend reports the record was created.
    /// Other files in the directory, such as the staging file of a write that never finished,
    /// are not streams.
    pub(crate) async fn records(&self) -> Result<Vec<(WriteStreamId, SystemTime)>, EngineError> {
        Ok(self
            .backend
            .list(&Self::directory())
            .await?
            .into_iter()
            .filter_map(|listed| match listed.kind {
                ListedKind::Object { created } => {
                    Some((listed.name.strip_suffix(".json")?.parse().ok()?, created))
                }
                ListedKind::Directory => None,
            })
            .collect())
    }

    /// Writes one acknowledged request of the stream, offsets `first` to `last`, as a new
    /// segment; durable once this returns.
    pub(crate) async fn write_segment(
        &self,
        stream: WriteStreamId,
        first: RowOffset,
        last: RowOffset,
        files: &SegmentFiles,
    ) -> Result<(), EngineError> {
        let name = format!("seg-{first}-{last}-{}.json", Uuid::new_v4());
        self.put(&Self::segments_directory(stream).join(name), files)
            .await
    }

    /// The stream's segments, in no order.
    pub(crate) async fn segments(
        &self,
        stream: WriteStreamId,
    ) -> Result<Vec<ListedSegment>, EngineError> {
        Ok(self
            .backend
            .list(&Self::segments_directory(stream))
            .await?
            .into_iter()
            .filter_map(|listed| match listed.kind {
                ListedKind::Object { created } => ListedSegment::parse(&listed.name, created),
                ListedKind::Directory => None,
            })
            .collect())
    }

    /// What `segment` of the stream holds, `None` once it is deleted.
    pub(crate) async fn read_segment(
        &self,
        stream: WriteStreamId,
        segment: &ListedSegment,
    ) -> Result<Option<SegmentFiles>, EngineError> {
        self.get(&Self::segments_directory(stream).join(&segment.name))
            .await
    }

    /// Deletes `segment` of the stream, logging a failure: its callers delete a segment that no
    /// commit takes any more.
    pub(crate) async fn delete_segment(&self, stream: WriteStreamId, segment: &ListedSegment) {
        let path = Self::segments_directory(stream).join(&segment.name);
        if let Err(err) = self.backend.delete(&path).await {
            warn!(%path, %err, "A write stream's segment could not be deleted.");
        }
    }

    /// Deletes the stream's segments, then its record.
    pub(crate) async fn delete_stream(&self, stream: WriteStreamId) -> Result<(), EngineError> {
        self.backend
            .delete_dir(&Self::segments_directory(stream))
            .await?;
        Ok(self.backend.delete(&Self::record(stream)).await?)
    }

    /// Deletes the data file at an Iceberg location, logging a failure: its callers delete
    /// files no snapshot references, which the orphan sweep removes otherwise.
    pub(crate) async fn delete_data_file(&self, location: &str) {
        let deleted = match self.backend.object_path(location) {
            Ok(path) => self.backend.delete(&path).await,
            Err(err) => Err(err),
        };
        if let Err(err) = deleted {
            warn!(location, %err, "A staged data file could not be deleted.");
        }
    }

    async fn put(&self, path: &ObjectPath, value: &impl Serialize) -> Result<(), EngineError> {
        let bytes = serde_json::to_vec(value).map_err(|err| EngineError::Internal {
            detail: format!("{path} does not serialize: {err}"),
        })?;
        Ok(self.backend.write(path, Bytes::from(bytes)).await?)
    }

    async fn get<T: serde::de::DeserializeOwned>(
        &self,
        path: &ObjectPath,
    ) -> Result<Option<T>, EngineError> {
        let Some(bytes) = self.backend.read(path).await? else {
            return Ok(None);
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|err| EngineError::Internal {
                detail: format!("{path} does not parse: {err}"),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn segment(first: u64, last: u64, created_s: u64) -> ListedSegment {
        ListedSegment {
            first: RowOffset::default().after(first),
            last: RowOffset::default().after(last),
            name: format!("seg-{first}-{last}-{created_s}.json"),
            created: SystemTime::UNIX_EPOCH + Duration::from_secs(created_s),
        }
    }

    #[test]
    fn a_commit_takes_one_segment_per_range_above_the_flushed_offset() {
        let resent_later = segment(0, 3, 2);
        let resent_first = segment(0, 3, 1);
        let split_original = segment(4, 7, 3);
        let lower_half = segment(4, 5, 4);
        let upper_half = segment(6, 7, 5);
        let after = segment(8, 11, 6);
        let all = vec![
            after.clone(),
            upper_half.clone(),
            resent_later,
            lower_half,
            split_original.clone(),
            resent_first.clone(),
        ];

        let taken = select_segments(all.clone(), None).expect("the segments line up");
        assert_eq!(taken, [resent_first, split_original, after.clone()]);

        let flushed = RowOffset::default().after(5);
        let taken = select_segments(all, Some(flushed)).expect("the segments line up");
        assert_eq!(taken, [upper_half, after]);

        let gap = select_segments(vec![segment(0, 3, 1), segment(5, 7, 2)], None);
        assert_eq!(gap, Err(segment(5, 7, 2)));
        let overlap = select_segments(vec![segment(0, 3, 1), segment(2, 5, 2)], None);
        assert_eq!(overlap, Err(segment(2, 5, 2)));
    }
}
