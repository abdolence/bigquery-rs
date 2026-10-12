//! Snapshots the engine builds itself, for the changes no transaction action of iceberg 0.10.1
//! makes: a `replace` that merges manifests and touches no data file, a `delete` that removes
//! every live data file, and an `overwrite` that removes them and adds others.
//!
//! A snapshot is built on one pointer and committed on top of that pointer only, under
//! `RefSnapshotIdMatch`: an attempt that loads another pointer builds it again, so what a
//! truncate or an overwrite removes is what the table holds at its commit, rows appended
//! meanwhile included.

use super::commit::{AttemptChange, Lost};
use super::{Change, EngineCatalog, EngineCommit, LoadedTable};
use crate::error::{EngineError, Resource};
use crate::ids::TableRef;
use futures::{stream, StreamExt, TryStreamExt};
use iceberg::spec::{
    DataFile, FormatVersion, ManifestContentType, ManifestEntry, ManifestEntryRef, ManifestFile,
    ManifestListWriter, ManifestWriter, ManifestWriterBuilder, Operation, Snapshot, SnapshotRef,
    SnapshotReference, SnapshotRetention, SnapshotSummaryCollector, Summary, TableMetadata,
    MAIN_BRANCH,
};
use iceberg::table::Table;
use iceberg::{TableRequirement, TableUpdate};
use std::collections::HashMap;
use std::fmt::Arguments;
use std::num::NonZeroUsize;
use uuid::Uuid;

/// What an engine-built snapshot makes of the table's current snapshot.
#[derive(Debug)]
pub(crate) enum SnapshotChange {
    /// A `replace` snapshot: the live entries in as few manifests as
    /// `manifest_merge_target_entries` allows, every data file kept.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the maintenance merge is its caller and is not built yet"
        )
    )]
    MergeManifests,
    /// A `delete` snapshot that removes every live data file.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "TRUNCATE TABLE is its caller and is not built yet"
        )
    )]
    Truncate,
    /// An `overwrite` snapshot that removes every live data file and adds `files`, written for
    /// the table identity `table_uuid`; an `append` when the table holds no live file.
    Overwrite {
        table_uuid: Uuid,
        files: Vec<DataFile>,
    },
}

/// A snapshot built on one pointer and written, not yet committed.
#[derive(Debug)]
pub(crate) struct BuiltSnapshot {
    base_metadata_location: String,
    requirements: Vec<TableRequirement>,
    /// `None` when the change leaves the table as it is.
    snapshot: Option<UnstampedSnapshot>,
    /// The manifests and the manifest list, which nothing names until the commit lands.
    pub(super) written: Vec<String>,
    removed_rows: u64,
}

// The attempt that commits the snapshot sets its timestamp, so that a build that waited for
// the table lock is not stamped with the time before the wait.
#[derive(Debug)]
struct UnstampedSnapshot {
    snapshot_id: i64,
    parent_snapshot_id: Option<i64>,
    sequence_number: i64,
    manifest_list: String,
    summary: Summary,
    schema_id: i32,
}

// `TableMetadataBuilder::add_snapshot` refuses a snapshot stamped further behind the table's
// last update or last snapshot.
const CLOCK_TOLERANCE_MS: i64 = 60_000;

impl BuiltSnapshot {
    /// Whether the change leaves the table as it is.
    pub(super) fn is_unchanged(&self) -> bool {
        self.snapshot.is_none()
    }

    /// The commit of the snapshot stamped `timestamp_ms`, built for `metadata`, the table the
    /// attempt loaded.
    ///
    /// # Errors
    /// [`Lost::ClockBehind`] when `timestamp_ms` is further behind the table's last update or
    /// last snapshot than iceberg accepts: another process with a clock ahead of this host's
    /// committed last.
    pub(super) fn commit_at(
        &self,
        metadata: &TableMetadata,
        timestamp_ms: i64,
    ) -> Result<EngineCommit, Lost> {
        let Some(unstamped) = &self.snapshot else {
            return Ok(EngineCommit {
                requirements: self.requirements.clone(),
                updates: Vec::new(),
            });
        };
        let latest = metadata
            .history()
            .last()
            .map_or(i64::MIN, |entry| entry.timestamp_ms)
            .max(metadata.last_updated_ms());
        if timestamp_ms < latest.saturating_sub(CLOCK_TOLERANCE_MS) {
            return Err(Lost::ClockBehind);
        }
        let snapshot = Snapshot::builder()
            .with_snapshot_id(unstamped.snapshot_id)
            .with_parent_snapshot_id(unstamped.parent_snapshot_id)
            .with_sequence_number(unstamped.sequence_number)
            .with_timestamp_ms(timestamp_ms)
            .with_manifest_list(unstamped.manifest_list.clone())
            .with_summary(unstamped.summary.clone())
            .with_schema_id(unstamped.schema_id)
            .build();
        Ok(EngineCommit {
            requirements: self.requirements.clone(),
            updates: vec![
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference: SnapshotReference::new(
                        unstamped.snapshot_id,
                        SnapshotRetention::branch(None, None, None),
                    ),
                },
            ],
        })
    }
}

#[derive(Default)]
struct CurrentManifests {
    data_manifests: usize,
    // Only a merge drops a manifest without a live entry.
    dead_data_manifests: usize,
    live_entries: Vec<ManifestEntryRef>,
    delete_manifests: Vec<ManifestFile>,
    // Every engine-built snapshot carries the delete files over unchanged.
    delete_file_bytes: u64,
}

impl CurrentManifests {
    async fn read(
        table: &Table,
        snapshot: &SnapshotRef,
        loaded: &mut LoadedManifests,
    ) -> Result<Self, EngineError> {
        let list = table.manifest_list_reader(snapshot).load().await?;
        loaded.load(table, list.entries().iter()).await?;
        let mut current = Self::default();
        for manifest in list.entries() {
            match manifest.content {
                ManifestContentType::Deletes => {
                    current.delete_file_bytes += loaded
                        .live_entries(manifest)?
                        .iter()
                        .map(|entry| entry.data_file().file_size_in_bytes())
                        .sum::<u64>();
                    current.delete_manifests.push(manifest.clone());
                }
                ManifestContentType::Data => {
                    let live = loaded.live_entries(manifest)?;
                    current.data_manifests += 1;
                    if live.is_empty() {
                        current.dead_data_manifests += 1;
                    }
                    current.live_entries.extend_from_slice(live);
                }
            }
        }
        Ok(current)
    }
}

const CONCURRENT_MANIFEST_READS: usize = 16;

// The live entries of the manifests a commit has read, by manifest path. A manifest is never
// rewritten under its path, so what one attempt read holds for every later one.
#[derive(Default)]
struct LoadedManifests(HashMap<String, Vec<ManifestEntryRef>>);

impl LoadedManifests {
    async fn load<'m>(
        &mut self,
        table: &Table,
        manifests: impl Iterator<Item = &'m ManifestFile>,
    ) -> Result<(), EngineError> {
        // Owned values: a stream over borrowed manifests makes the commit future not `Send`.
        let unread: Vec<ManifestFile> = manifests
            .filter(|manifest| !self.0.contains_key(&manifest.manifest_path))
            .cloned()
            .collect();
        let read: Vec<(String, Vec<ManifestEntryRef>)> = stream::iter(unread)
            .map(|manifest| {
                let file_io = table.file_io().clone();
                async move {
                    let (entries, _) = manifest.load_manifest(&file_io).await?.into_parts();
                    let live = entries.into_iter().filter(|entry| entry.is_alive());
                    Ok::<_, EngineError>((manifest.manifest_path, live.collect()))
                }
            })
            .buffered(CONCURRENT_MANIFEST_READS)
            .try_collect()
            .await?;
        self.0.extend(read);
        Ok(())
    }

    fn live_entries(&self, manifest: &ManifestFile) -> Result<&[ManifestEntryRef], EngineError> {
        self.0
            .get(&manifest.manifest_path)
            .map(Vec::as_slice)
            .ok_or_else(|| EngineError::Internal {
                detail: format!("the manifest {} was not read", manifest.manifest_path),
            })
    }
}

/// What a snapshot keeps, removes and adds of its parent's live data files.
struct Rewrite<'c> {
    operation: Operation,
    kept: &'c [ManifestEntryRef],
    removed: &'c [ManifestEntryRef],
    added: &'c [DataFile],
    delete_file_bytes: u64,
}

impl Rewrite<'_> {
    fn summary(&self, metadata: &TableMetadata) -> Summary {
        let schema = metadata.current_schema();
        let spec = metadata.default_partition_spec();
        let mut collector = SnapshotSummaryCollector::default();
        for entry in self.removed {
            collector.remove_file(entry.data_file(), schema.clone(), spec.clone());
        }
        for file in self.added {
            collector.add_file(file, schema.clone(), spec.clone());
        }
        let mut properties = collector.build();
        let live: Vec<&DataFile> = self
            .kept
            .iter()
            .map(|entry| entry.data_file())
            .chain(self.added)
            .collect();
        let records: u64 = live.iter().map(|file| file.record_count()).sum();
        let bytes: u64 = live
            .iter()
            .map(|file| file.file_size_in_bytes())
            .sum::<u64>()
            + self.delete_file_bytes;
        for (key, total) in [
            ("total-data-files", live.len().to_string()),
            ("total-records", records.to_string()),
            ("total-files-size", bytes.to_string()),
        ] {
            properties.insert(key.to_string(), total);
        }
        // Delete files are carried over as they are, so their totals are the parent's.
        for key in [
            "total-delete-files",
            "total-position-deletes",
            "total-equality-deletes",
        ] {
            let total = match metadata.current_snapshot() {
                Some(parent) => parent.summary().additional_properties.get(key).cloned(),
                None => Some("0".to_string()),
            };
            if let Some(total) = total {
                properties.insert(key.to_string(), total);
            }
        }
        Summary {
            operation: self.operation.clone(),
            additional_properties: properties,
        }
    }
}

#[derive(Clone, Copy)]
enum ManifestRecord<'e> {
    Existing(&'e ManifestEntry),
    Deleted(&'e ManifestEntry),
    Added(&'e DataFile),
}

impl ManifestRecord<'_> {
    fn write_to(
        self,
        writer: &mut ManifestWriter,
        sequence_number: i64,
    ) -> Result<(), EngineError> {
        match self {
            ManifestRecord::Existing(entry) => {
                let (added_by, sequence) = added_by_and_sequence(entry)?;
                writer.add_existing_file(
                    entry.data_file().clone(),
                    added_by,
                    sequence,
                    entry.file_sequence_number,
                )?;
            }
            ManifestRecord::Deleted(entry) => {
                let (_, sequence) = added_by_and_sequence(entry)?;
                writer.add_delete_file(
                    entry.data_file().clone(),
                    sequence,
                    entry.file_sequence_number,
                )?;
            }
            ManifestRecord::Added(file) => writer.add_file(file.clone(), sequence_number)?,
        }
        Ok(())
    }
}

fn added_by_and_sequence(entry: &ManifestEntry) -> Result<(i64, i64), EngineError> {
    entry
        .snapshot_id()
        .zip(entry.sequence_number())
        .ok_or_else(|| EngineError::Internal {
            detail: format!(
                "the manifest entry of {} has no snapshot id or sequence number",
                entry.file_path()
            ),
        })
}

struct SnapshotWriter<'t> {
    table: &'t Table,
    snapshot_id: i64,
    sequence_number: i64,
    target_entries: NonZeroUsize,
    commit: Uuid,
    written: Vec<String>,
}

impl SnapshotWriter<'_> {
    fn location(&self, name: Arguments<'_>) -> String {
        format!("{}/metadata/{name}", self.table.metadata().location())
    }

    /// Writes the manifests of `rewrite` beside the parent's `delete_manifests`, and the
    /// manifest list naming them, whose location it returns.
    async fn write(
        &mut self,
        rewrite: &Rewrite<'_>,
        delete_manifests: &[ManifestFile],
    ) -> Result<String, EngineError> {
        let groups: [Vec<ManifestRecord>; 3] = [
            rewrite
                .kept
                .iter()
                .map(|entry| ManifestRecord::Existing(entry))
                .collect(),
            rewrite
                .removed
                .iter()
                .map(|entry| ManifestRecord::Deleted(entry))
                .collect(),
            rewrite.added.iter().map(ManifestRecord::Added).collect(),
        ];
        let mut manifests = Vec::new();
        for group in &groups {
            for entries in group.chunks(self.target_entries.get()) {
                manifests.push(self.manifest(entries).await?);
            }
        }
        manifests.extend_from_slice(delete_manifests);
        let location = self.location(format_args!(
            "snap-{}-0-{}.avro",
            self.snapshot_id, self.commit
        ));
        let output = self.table.file_io().new_output(&location)?;
        self.written.push(location.clone());
        let mut writer = ManifestListWriter::v2(
            output.writer().await?,
            self.snapshot_id,
            self.table.metadata().current_snapshot_id(),
            self.sequence_number,
        );
        writer.add_manifests(manifests.into_iter())?;
        writer.close().await?;
        Ok(location)
    }

    async fn manifest(
        &mut self,
        entries: &[ManifestRecord<'_>],
    ) -> Result<ManifestFile, EngineError> {
        let location = self.location(format_args!("{}-m{}.avro", self.commit, self.written.len()));
        let output = self.table.file_io().new_output(&location)?;
        self.written.push(location);
        let metadata = self.table.metadata();
        let mut writer = ManifestWriterBuilder::new(
            output,
            Some(self.snapshot_id),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().as_ref().clone(),
        )
        .build_v2_data();
        for entry in entries {
            entry.write_to(&mut writer, self.sequence_number)?;
        }
        Ok(writer.write_manifest_file().await?)
    }
}

fn new_snapshot_id(metadata: &TableMetadata) -> i64 {
    loop {
        let id = rand::random_range(1..=i64::MAX);
        if metadata.snapshot_by_id(id).is_none() {
            return id;
        }
    }
}

impl SnapshotChange {
    fn rewrite<'c>(
        &'c self,
        current: &'c CurrentManifests,
        target_entries: NonZeroUsize,
    ) -> Option<Rewrite<'c>> {
        let live = current.live_entries.as_slice();
        let (kept, removed, added): (&[ManifestEntryRef], &[ManifestEntryRef], &[DataFile]) =
            match self {
                SnapshotChange::MergeManifests => (live, &[], &[]),
                SnapshotChange::Truncate => (&[], live, &[]),
                SnapshotChange::Overwrite { files, .. } => (&[], live, files),
            };
        let operation = match (self, removed.is_empty(), added.is_empty()) {
            (SnapshotChange::MergeManifests, ..) => (current.dead_data_manifests > 0
                || kept.len().div_ceil(target_entries.get()) < current.data_manifests)
                .then_some(Operation::Replace)?,
            (_, true, true) => return None,
            (_, false, false) => Operation::Overwrite,
            (_, false, true) => Operation::Delete,
            (_, true, false) => Operation::Append,
        };
        Some(Rewrite {
            operation,
            kept,
            removed,
            added,
            delete_file_bytes: current.delete_file_bytes,
        })
    }

    async fn build(
        &self,
        table: &TableRef,
        loaded: &LoadedTable,
        target_entries: NonZeroUsize,
        manifests: &mut LoadedManifests,
    ) -> Result<BuiltSnapshot, EngineError> {
        let metadata = loaded.metadata();
        if let SnapshotChange::Overwrite { table_uuid, .. } = self {
            if *table_uuid != metadata.uuid() {
                return Err(EngineError::NotFound {
                    what: Resource::Table(table.clone()),
                });
            }
        }
        if metadata.format_version() != FormatVersion::V2 {
            return Err(EngineError::unsupported(
                format!("an Iceberg {} table", metadata.format_version()),
                format!("{table}: the engine builds snapshots of v2 tables only"),
            ));
        }
        let specs = metadata.partition_specs_iter().len();
        if specs > 1 {
            return Err(EngineError::unsupported(
                "a table with more than one partition spec",
                format!("{table} has {specs}"),
            ));
        }
        let parent = metadata.current_snapshot();
        let parent_id = parent.map(|parent| parent.snapshot_id());
        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: parent_id,
            },
        ];
        let current = match parent {
            Some(parent) => CurrentManifests::read(&loaded.table, parent, manifests).await?,
            None => CurrentManifests::default(),
        };
        let Some(rewrite) = self.rewrite(&current, target_entries) else {
            return Ok(BuiltSnapshot {
                base_metadata_location: loaded.metadata_location().to_string(),
                requirements,
                snapshot: None,
                written: Vec::new(),
                removed_rows: 0,
            });
        };
        let mut snapshot_writer = SnapshotWriter {
            table: &loaded.table,
            snapshot_id: new_snapshot_id(metadata),
            sequence_number: metadata.next_sequence_number(),
            target_entries,
            commit: Uuid::new_v4(),
            written: Vec::new(),
        };
        let manifest_list = snapshot_writer
            .write(&rewrite, &current.delete_manifests)
            .await?;
        Ok(BuiltSnapshot {
            base_metadata_location: loaded.metadata_location().to_string(),
            requirements,
            snapshot: Some(UnstampedSnapshot {
                snapshot_id: snapshot_writer.snapshot_id,
                parent_snapshot_id: parent_id,
                sequence_number: snapshot_writer.sequence_number,
                manifest_list,
                summary: rewrite.summary(metadata),
                schema_id: metadata.current_schema_id(),
            }),
            written: snapshot_writer.written,
            removed_rows: rewrite
                .removed
                .iter()
                .map(|entry| entry.record_count())
                .sum(),
        })
    }
}

struct SnapshotAttempts<'c> {
    catalog: &'c EngineCatalog,
    table: &'c TableRef,
    change: &'c SnapshotChange,
    prepared: Option<BuiltSnapshot>,
    manifests: LoadedManifests,
    removed_rows: u64,
}

impl AttemptChange for &mut SnapshotAttempts<'_> {
    async fn for_attempt(&mut self, loaded: &LoadedTable) -> Result<Change, EngineError> {
        let built = match self.prepared.take() {
            Some(built) if built.base_metadata_location == loaded.metadata_location() => built,
            stale => {
                if let Some(stale) = stale {
                    self.catalog.discard(&stale.written).await;
                }
                let target_entries = self.catalog.settings.manifest_merge_target_entries;
                self.change
                    .build(self.table, loaded, target_entries, &mut self.manifests)
                    .await?
            }
        };
        self.removed_rows = built.removed_rows;
        Ok(Change::Snapshot(built))
    }
}

impl EngineCatalog {
    /// Commits the snapshot `change` makes of `table`'s current snapshot, and returns the rows of
    /// the data files it removed.
    ///
    /// The snapshot is first built outside the table lock, on the table as loaded, and an
    /// attempt that loads another pointer builds it again under the lock.
    ///
    /// # Errors
    /// [`EngineError::NotFound`] when the table does not exist or, for an overwrite, is no
    /// longer the identity its files were written for; [`EngineError::Unsupported`] for a table
    /// of another format version than 2 or with more than one partition spec; and the commit
    /// loop's errors.
    pub(crate) async fn commit_snapshot(
        &self,
        table: &TableRef,
        change: &SnapshotChange,
    ) -> Result<u64, EngineError> {
        let target_entries = self.settings.manifest_merge_target_entries;
        let loaded = self.load(table).await?;
        let mut manifests = LoadedManifests::default();
        let prepared = change
            .build(table, &loaded, target_entries, &mut manifests)
            .await?;
        let mut attempts = SnapshotAttempts {
            catalog: self,
            table,
            change,
            prepared: Some(prepared),
            manifests,
            removed_rows: 0,
        };
        let committed = self.commit(table, &mut attempts).await;
        if let Some(unused) = attempts.prepared.take() {
            self.discard(&unused.written).await;
        }
        committed?;
        Ok(attempts.removed_rows)
    }

    /// Deletes files an attempt wrote and nothing names, logging a failure: the orphan sweep
    /// removes them otherwise.
    pub(super) async fn discard(&self, written: &[String]) {
        for location in written {
            self.delete_location(location).await;
        }
    }
}

#[cfg(test)]
mod tests;
