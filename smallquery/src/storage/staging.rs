//! What the root holds of the write streams `CreateWriteStream` makes:
//!
//! - the stream record, `_smallquery/streams/<id>.json`, written before the stream is
//!   reported;
//! - a pending or buffered stream's segments,
//!   `_smallquery/streams/<id>/seg-<first>-<last>-<uuid>.json`, one per acknowledged request,
//!   written before the acknowledgement and never rewritten;
//! - a pending or buffered stream's table properties `smallquery.stream.<id>.state`,
//!   `.flushed` and `.commit_time`, from the commit that registers the stream to the one that
//!   drops it.
//!
//! A staged data file lives in the table's `data/` directory under its final name, and no
//! snapshot references it until a flush or a batch commit adds it.

use super::{Backend, ListedKind, ObjectPath};
use crate::error::{EngineError, StreamProblem};
use crate::ids::{NodeId, RowOffset, TableRef, WriteStreamId};
use bytes::Bytes;
use iceberg::spec::{
    deserialize_data_file_from_json, serialize_data_file_to_json, DataFile, TableMetadata,
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::ops::Range;
use std::time::SystemTime;
use tracing::warn;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StreamRecord {
    pub mode: StreamMode,
    pub table: TableRef,
    /// A table dropped and created again under the name is another table, which the stream's
    /// rows never reach.
    pub table_uuid: Uuid,
    pub created_at_ms: i64,
    pub creator: NodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StreamMode {
    Committed,
    Pending,
    Buffered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamProperty {
    State,
    Flushed,
    CommitTime,
}

impl StreamProperty {
    fn key(self, stream: WriteStreamId) -> String {
        let name = match self {
            StreamProperty::State => "state",
            StreamProperty::Flushed => "flushed",
            StreamProperty::CommitTime => "commit_time",
        };
        format!("smallquery.stream.{stream}.{name}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamState {
    Open,
    /// Pending streams only; a buffered stream's finalize stays with its creator.
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

/// What a table holds of a pending or buffered stream: a stream whose state is absent is gone,
/// and no commit takes its rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StoredStream {
    pub state: StreamState,
    pub flushed: Option<RowOffset>,
    pub committed_at_ms: Option<i64>,
}

impl StoredStream {
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
            committed_at_ms: value(StreamProperty::CommitTime)
                .map(|time| {
                    time.parse().map_err(|_| EngineError::Internal {
                        detail: format!("{time:?} is no commit time"),
                    })
                })
                .transpose()?,
        }))
    }

    pub(crate) fn properties(self, stream: WriteStreamId) -> Vec<(String, String)> {
        let mut properties = vec![(
            StreamProperty::State.key(stream),
            self.state.as_str().to_string(),
        )];
        properties.extend(
            self.flushed
                .map(|flushed| (StreamProperty::Flushed.key(stream), flushed.to_string())),
        );
        properties.extend(
            self.committed_at_ms
                .map(|time| (StreamProperty::CommitTime.key(stream), time.to_string())),
        );
        properties
    }

    pub(crate) fn keys(stream: WriteStreamId) -> [String; 3] {
        [
            StreamProperty::State.key(stream),
            StreamProperty::Flushed.key(stream),
            StreamProperty::CommitTime.key(stream),
        ]
    }
}

/// A change of a stream's state, made by a commit only on the state it starts from, so that
/// whichever engine commits it, the table holds exactly what the stream has committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamChange {
    Register(WriteStreamId),
    Finalize(WriteStreamId),
    /// `at_ms` is what `GetWriteStream` reports on any engine, so that a client that sends a
    /// landed batch commit again can tell when it landed.
    Commit {
        stream: WriteStreamId,
        at_ms: i64,
    },
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
            | StreamChange::Commit { stream, .. }
            | StreamChange::Flush { stream, .. } => stream,
        }
    }

    pub(crate) fn apply(
        self,
        current: Option<StoredStream>,
    ) -> Result<StoredStream, StreamProblem> {
        let Some(current) = current else {
            return match self {
                StreamChange::Register(_) => Ok(StoredStream {
                    state: StreamState::Open,
                    flushed: None,
                    committed_at_ms: None,
                }),
                _ => Err(StreamProblem::Expired),
            };
        };
        match (self, current.state) {
            (StreamChange::Finalize(_), StreamState::Open) => Ok(StoredStream {
                state: StreamState::Finalized,
                ..current
            }),
            (StreamChange::Commit { at_ms, .. }, StreamState::Finalized) => Ok(StoredStream {
                state: StreamState::Committed,
                committed_at_ms: Some(at_ms),
                ..current
            }),
            (StreamChange::Commit { .. }, StreamState::Committed) => {
                Err(StreamProblem::AlreadyCommitted)
            }
            (StreamChange::Commit { .. }, StreamState::Open) => Err(StreamProblem::NotFinalized),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ListedSegment {
    pub first: RowOffset,
    pub last: RowOffset,
    pub name: String,
    pub created: SystemTime,
}

impl ListedSegment {
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

/// A stream's segments, in no order.
#[derive(Debug)]
pub(crate) struct Segments(Vec<ListedSegment>);

impl Segments {
    /// The segments a commit takes, in offset order: none at or below `flushed`, and one per
    /// range, since a request sent again, or the halves of a split whose original survived,
    /// stage the same rows twice.
    ///
    /// # Errors
    /// The segment that leaves a gap or overlaps without being covered: no order of the
    /// engine's writes produces either.
    pub(crate) fn select(
        self,
        flushed: Option<RowOffset>,
    ) -> Result<Vec<ListedSegment>, ListedSegment> {
        let mut segments = self.0;
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
}

impl IntoIterator for Segments {
    type Item = ListedSegment;
    type IntoIter = std::vec::IntoIter<ListedSegment>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SegmentFiles {
    pub files: Vec<StagedFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StagedFile {
    pub path: String,
    /// In the file's row order.
    pub rows: OffsetRanges,
    pub schema_id: i32,
    pub data_file: Box<RawValue>,
}

impl StagedFile {
    /// `file` must be written with `metadata`'s current schema and default partition spec.
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
            data_file: RawValue::from_string(json).map_err(|err| EngineError::Internal {
                detail: format!("a data file's JSON does not parse: {err}"),
            })?,
        })
    }

    /// `metadata` is the metadata of the table identity the file was written for.
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
            self.data_file.get(),
            spec.spec_id(),
            &spec.partition_type(schema)?,
            schema,
        )?)
    }
}

/// Offsets in ascending order, as runs of consecutive ones. A file of one partition holds a run
/// for each stretch of a request's rows that fell into that partition.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct OffsetRanges(Vec<Range<RowOffset>>);

impl OffsetRanges {
    pub(crate) fn from_ascending(offsets: impl IntoIterator<Item = RowOffset>) -> Self {
        let mut ranges: Vec<Range<RowOffset>> = Vec::new();
        for offset in offsets {
            match ranges.last_mut() {
                Some(last) if last.end == offset => last.end = offset.after(1),
                _ => ranges.push(offset..offset.after(1)),
            }
        }
        Self(ranges)
    }

    pub(crate) fn rows(&self) -> u64 {
        self.0
            .iter()
            .map(|range| range.start.rows_to(range.end))
            .sum()
    }

    pub(crate) fn rows_before(&self, end: RowOffset) -> u64 {
        self.0
            .iter()
            .map(|range| range.start.rows_to(range.end.min(end)))
            .sum()
    }

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
                head.push(range.start..cut);
                tail.push(cut..range.end);
                left = 0;
            }
        }
        (Self(head), Self(tail))
    }
}

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

    /// Durable once this returns.
    pub(crate) async fn write_record(
        &self,
        stream: WriteStreamId,
        record: &StreamRecord,
    ) -> Result<(), EngineError> {
        self.put(&Self::record(stream), record).await
    }

    pub(crate) async fn read_record(
        &self,
        stream: WriteStreamId,
    ) -> Result<Option<StreamRecord>, EngineError> {
        self.get(&Self::record(stream)).await
    }

    /// Every stream with a record, with the time the backend reports the record was created.
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

    /// Durable once this returns.
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

    pub(crate) async fn segments(&self, stream: WriteStreamId) -> Result<Segments, EngineError> {
        Ok(Segments(
            self.backend
                .list(&Self::segments_directory(stream))
                .await?
                .into_iter()
                .filter_map(|listed| match listed.kind {
                    ListedKind::Object { created } => ListedSegment::parse(&listed.name, created),
                    ListedKind::Directory => None,
                })
                .collect(),
        ))
    }

    pub(crate) async fn read_segment(
        &self,
        stream: WriteStreamId,
        segment: &ListedSegment,
    ) -> Result<Option<SegmentFiles>, EngineError> {
        self.get(&Self::segments_directory(stream).join(&segment.name))
            .await
    }

    /// Logs a failure: callers delete only a segment no commit takes any more.
    pub(crate) async fn delete_segment(&self, stream: WriteStreamId, segment: &ListedSegment) {
        let path = Self::segments_directory(stream).join(&segment.name);
        if let Err(err) = self.backend.delete(&path).await {
            warn!(%path, %err, "A write stream's segment could not be deleted.");
        }
    }

    pub(crate) async fn delete_stream(&self, stream: WriteStreamId) -> Result<(), EngineError> {
        self.backend
            .delete_dir(&Self::segments_directory(stream))
            .await?;
        Ok(self.backend.delete(&Self::record(stream)).await?)
    }

    /// Logs a failure: callers delete only files no snapshot references, which the orphan
    /// sweep removes otherwise.
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

        let taken = Segments(all.clone())
            .select(None)
            .expect("the segments line up");
        assert_eq!(taken, [resent_first, split_original, after.clone()]);

        let flushed = RowOffset::default().after(5);
        let taken = Segments(all)
            .select(Some(flushed))
            .expect("the segments line up");
        assert_eq!(taken, [upper_half, after]);

        let gap = Segments(vec![segment(0, 3, 1), segment(5, 7, 2)]).select(None);
        assert_eq!(gap, Err(segment(5, 7, 2)));
        let overlap = Segments(vec![segment(0, 3, 1), segment(2, 5, 2)]).select(None);
        assert_eq!(overlap, Err(segment(2, 5, 2)));
    }
}
