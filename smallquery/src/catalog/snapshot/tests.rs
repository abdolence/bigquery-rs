//! Engine-built snapshots read back by Iceberg's own scan planning and by queries through the
//! `bigquery` client: a merge keeps every file and total in fewer manifests, a truncate removes
//! every live file while time travel still finds them, a truncate that loses to another
//! engine's appends removes their rows too, and open write streams outlive both kinds that
//! remove rows.

use super::SnapshotChange;
use crate::catalog::commit::Lost;
use crate::catalog::{now_ms, CommitPoint, LoadedTable};
use crate::ids::TableRef;
use crate::testkit::{table_ref, TestEngine, PROJECT};
use bigquery::*;
use futures::TryStreamExt;
use iceberg::spec::{Manifest, ManifestContentType, ManifestStatus, Operation, Summary};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
const EVENTS: BigQueryTableId = BigQueryTableId::from_static("events");

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Event {
    id: i64,
    amount: i64,
}

/// A file task as Iceberg's scan plans it.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PlannedTask {
    data_file_path: String,
    start: u64,
    length: u64,
    record_count: Option<u64>,
}

/// A manifest entry by its file and the numbers it keeps.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EntryNumbers {
    file_path: String,
    snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
}

fn events() -> TableRef {
    table_ref("shop", "events")
}

/// `shop.events` in `EU`, empty, with the columns of `SELECT 1 AS id, 2 AS amount`.
async fn create_events(engine: &TestEngine) {
    engine.create_dataset("shop", "EU").await;
    engine
        .db
        .fluent()
        .schema()
        .table(SHOP.table(EVENTS))
        .columns(|columns| {
            columns.fields([columns.field("id").int64(), columns.field("amount").int64()])
        })
        .sync()
        .await
        .expect("the table is created");
}

/// The events `ids`, each with ten times its id as the amount.
fn rows(ids: Range<i64>) -> Vec<Event> {
    ids.map(|id| Event {
        id,
        amount: id * 10,
    })
    .collect()
}

/// Inserts the events `ids` in one request, which lands as one data file in one manifest.
async fn insert(engine: &TestEngine, ids: Range<i64>) {
    engine
        .db
        .fluent()
        .insert()
        .into(SHOP.table(EVENTS))
        .objects(&rows(ids))
        .execute()
        .await
        .expect("the rows are written");
}

/// `COUNT(*)` and `SUM(amount)` of `shop.events`, queried through the client.
async fn count_and_sum(engine: &TestEngine) -> (i64, Option<i64>) {
    #[derive(Debug, Deserialize)]
    struct Aggregates {
        n: i64,
        total: Option<i64>,
    }
    let queried: Vec<Aggregates> = engine
        .db
        .fluent()
        .query("SELECT COUNT(*) AS n, SUM(amount) AS total FROM shop.events")
        .obj()
        .query()
        .await
        .expect("the table is queried");
    (queried[0].n, queried[0].total)
}

/// Every row of `shop.events` through Storage Read, sorted.
async fn read_events(engine: &TestEngine) -> Vec<Event> {
    let mut read: Vec<Event> = engine
        .db
        .fluent()
        .select()
        .from(SHOP.table(EVENTS))
        .obj()
        .query()
        .await
        .expect("the table reads");
    read.sort();
    read
}

async fn reload(engine: &TestEngine) -> LoadedTable {
    engine
        .state()
        .catalog
        .reload(&events())
        .await
        .expect("the table loads")
}

/// The file tasks Iceberg's own scan plans for `loaded` at `snapshot_id`.
async fn planned(loaded: &LoadedTable, snapshot_id: i64) -> BTreeSet<PlannedTask> {
    loaded
        .table
        .scan()
        .snapshot_id(snapshot_id)
        .build()
        .expect("the scan builds")
        .plan_files()
        .await
        .expect("the files plan")
        .map_ok(|task| PlannedTask {
            data_file_path: task.data_file_path,
            start: task.start,
            length: task.length,
            record_count: task.record_count,
        })
        .try_collect()
        .await
        .expect("every task plans")
}

/// The data manifests of `loaded`'s current snapshot.
async fn data_manifests(loaded: &LoadedTable) -> Vec<Manifest> {
    let snapshot = loaded
        .metadata()
        .current_snapshot()
        .expect("the table has a snapshot");
    let list = loaded
        .table
        .manifest_list_reader(snapshot)
        .load()
        .await
        .expect("the manifest list reads");
    let mut manifests = Vec::new();
    for file in list.entries() {
        if file.content == ManifestContentType::Data {
            manifests.push(
                file.load_manifest(loaded.table.file_io())
                    .await
                    .expect("the manifest reads"),
            );
        }
    }
    manifests
}

/// The entries of `manifests` with `status`, by the numbers each keeps.
fn entries(manifests: &[Manifest], status: ManifestStatus) -> BTreeSet<EntryNumbers> {
    manifests
        .iter()
        .flat_map(Manifest::entries)
        .filter(|entry| entry.status() == status)
        .map(|entry| EntryNumbers {
            file_path: entry.file_path().to_string(),
            snapshot_id: entry.snapshot_id(),
            sequence_number: entry.sequence_number(),
            file_sequence_number: entry.file_sequence_number,
        })
        .collect()
}

/// The `total-*` keys of a snapshot summary.
fn totals(summary: &Summary) -> BTreeMap<&str, &str> {
    summary
        .additional_properties
        .iter()
        .filter(|(key, _)| key.starts_with("total-"))
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect()
}

/// The Avro files under the table's metadata directory that no snapshot of `loaded` names,
/// neither as its manifest list nor as a manifest of that list.
async fn unnamed_avro_files(engine: &TestEngine, loaded: &LoadedTable) -> BTreeSet<String> {
    let mut named = BTreeSet::new();
    for snapshot in loaded.metadata().snapshots() {
        named.insert(snapshot.manifest_list().to_string());
        let list = loaded
            .table
            .manifest_list_reader(snapshot)
            .load()
            .await
            .expect("the manifest list reads");
        named.extend(list.entries().iter().map(|file| file.manifest_path.clone()));
    }
    let named: BTreeSet<String> = named
        .iter()
        .filter_map(|location| location.rsplit('/').next())
        .map(str::to_string)
        .collect();
    let directory = engine
        .root()
        .join(PROJECT)
        .join("shop")
        .join("events")
        .join("metadata");
    std::fs::read_dir(directory)
        .expect("the metadata directory lists")
        .map(|entry| {
            entry
                .expect("the directory lists")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|name| name.ends_with(".avro") && !named.contains(name))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merging_the_manifests_of_200_appends_keeps_every_file_row_and_total() {
    let engine = TestEngine::start().await;
    create_events(&engine).await;
    for id in 0..200 {
        insert(&engine, id..id + 1).await;
    }
    let before = reload(&engine).await;
    let parent = before
        .metadata()
        .current_snapshot()
        .expect("the appends made a snapshot")
        .clone();
    let manifests_before = data_manifests(&before).await;
    assert_eq!(manifests_before.len(), 200);
    let planned_before = planned(&before, parent.snapshot_id()).await;
    let queried_before = count_and_sum(&engine).await;
    assert_eq!(queried_before, (200, Some((0..200).sum::<i64>() * 10)));

    let removed_rows = engine
        .state()
        .catalog
        .commit_snapshot(&events(), &SnapshotChange::MergeManifests)
        .await
        .expect("the manifests merge");

    assert_eq!(removed_rows, 0);
    let after = reload(&engine).await;
    let snapshot = after
        .metadata()
        .current_snapshot()
        .expect("the merge made a snapshot");
    assert_eq!(snapshot.summary().operation, Operation::Replace);
    assert_eq!(snapshot.parent_snapshot_id(), Some(parent.snapshot_id()));
    let manifests_after = data_manifests(&after).await;
    assert_eq!(manifests_after.len(), 1);
    assert_eq!(
        entries(&manifests_after, ManifestStatus::Existing),
        [ManifestStatus::Added, ManifestStatus::Existing]
            .into_iter()
            .flat_map(|status| entries(&manifests_before, status))
            .collect(),
        "every live entry is kept as Existing with the numbers it had"
    );
    assert_eq!(
        planned(&after, snapshot.snapshot_id()).await,
        planned_before
    );
    assert_eq!(totals(snapshot.summary()), totals(parent.summary()));
    assert_eq!(count_and_sum(&engine).await, queried_before);
    assert_eq!(
        planned(&after, parent.snapshot_id()).await,
        planned_before,
        "time travel to the snapshot before the merge plans the same files"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_truncate_removes_every_live_file_and_time_travel_still_reads_them() {
    let engine = TestEngine::with_config(|mut config| {
        config.maintenance.manifest_merge_target_entries = 3;
        config
    })
    .await;
    create_events(&engine).await;
    for file in 0..10 {
        insert(&engine, file * 10..file * 10 + 10).await;
    }
    let before = reload(&engine).await;
    let parent = before
        .metadata()
        .current_snapshot()
        .expect("the appends made a snapshot")
        .clone();
    let live_before = entries(&data_manifests(&before).await, ManifestStatus::Added);
    let planned_before = planned(&before, parent.snapshot_id()).await;
    assert_eq!(planned_before.len(), 10);

    let removed_rows = engine
        .state()
        .catalog
        .commit_snapshot(&events(), &SnapshotChange::Truncate)
        .await
        .expect("the table is truncated");

    assert_eq!(removed_rows, 100);
    let after = reload(&engine).await;
    let snapshot = after
        .metadata()
        .current_snapshot()
        .expect("the truncate made a snapshot");
    assert_eq!(snapshot.summary().operation, Operation::Delete);
    assert_eq!(snapshot.sequence_number(), parent.sequence_number() + 1);
    let summary = &snapshot.summary().additional_properties;
    for (key, value) in [
        ("total-records", "0"),
        ("total-data-files", "0"),
        ("deleted-records", "100"),
        ("deleted-data-files", "10"),
    ] {
        assert_eq!(summary.get(key).map(String::as_str), Some(value), "{key}");
    }
    let manifests = data_manifests(&after).await;
    assert_eq!(
        manifests
            .iter()
            .map(|manifest| manifest.entries().len())
            .collect::<Vec<_>>(),
        [3, 3, 3, 1],
        "no manifest holds more than manifest_merge_target_entries"
    );
    let deleted = entries(&manifests, ManifestStatus::Deleted);
    assert_eq!(
        deleted,
        live_before
            .into_iter()
            .map(|kept| EntryNumbers {
                snapshot_id: Some(snapshot.snapshot_id()),
                ..kept
            })
            .collect(),
        "every removed file is a Deleted entry of the new snapshot with its own numbers"
    );
    assert!(planned(&after, snapshot.snapshot_id()).await.is_empty());
    assert_eq!(count_and_sum(&engine).await, (0, None));
    assert_eq!(
        planned(&after, parent.snapshot_id()).await,
        planned_before,
        "time travel to the snapshot before the truncate plans the removed files"
    );

    let snapshots = after.metadata().snapshots().len();
    let removed_again = engine
        .state()
        .catalog
        .commit_snapshot(&events(), &SnapshotChange::Truncate)
        .await
        .expect("an empty table truncates");
    assert_eq!(removed_again, 0);
    assert_eq!(
        reload(&engine).await.metadata().snapshots().len(),
        snapshots,
        "a table without live files gets no snapshot"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_truncate_that_loses_to_another_engine_s_appends_removes_their_rows_too() {
    let first = TestEngine::start().await;
    create_events(&first).await;
    insert(&first, 0..10).await;
    let second = first.open_on_same_root(|config| config).await;
    let mut held = second.state().catalog.hooks().hold(CommitPoint::Loaded);
    let catalog = second.state().catalog.clone();
    let truncate = tokio::spawn(async move {
        catalog
            .commit_snapshot(&events(), &SnapshotChange::Truncate)
            .await
    });
    held.reached().await;
    insert(&first, 10..15).await;
    insert(&first, 15..20).await;
    let appended = reload(&first)
        .await
        .metadata()
        .current_snapshot()
        .expect("the appends made a snapshot")
        .clone();
    held.release();

    let removed_rows = truncate
        .await
        .expect("the truncate task ends")
        .expect("the truncate lands");

    assert_eq!(removed_rows, 20);
    let loaded = reload(&first).await;
    let snapshot = loaded
        .metadata()
        .current_snapshot()
        .expect("the truncate made a snapshot");
    assert_eq!(snapshot.summary().operation, Operation::Delete);
    assert_eq!(snapshot.parent_snapshot_id(), Some(appended.snapshot_id()));
    assert_eq!(snapshot.sequence_number(), appended.sequence_number() + 1);
    let log: Vec<i64> = loaded
        .metadata()
        .history()
        .iter()
        .map(|entry| entry.snapshot_id)
        .collect();
    assert_eq!(
        log[log.len() - 2..],
        [appended.snapshot_id(), snapshot.snapshot_id()],
        "the snapshot log holds the appends and then the truncate"
    );
    assert!(planned(&loaded, snapshot.snapshot_id()).await.is_empty());
    assert_eq!(count_and_sum(&first).await, (0, None));
    assert_eq!(
        unnamed_avro_files(&first, &loaded).await,
        BTreeSet::new(),
        "the losing attempt's manifests and manifest list are deleted"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pending_stream_open_across_a_truncate_and_an_overwrite_commits_its_rows_after_them() {
    let engine = TestEngine::start().await;
    create_events(&engine).await;
    insert(&engine, 0..5).await;
    let (mut writer, _responses) = engine
        .db
        .create_streaming_writer_with_options::<Event>(
            SHOP.table(EVENTS),
            BigQueryStreamingWriteOptions::new().with_mode(BigQueryWriteMode::Pending),
        )
        .await
        .expect("the writer opens");
    writer
        .write_all(&rows(100..104))
        .await
        .expect("rows are queued");
    let pending = writer.finalize().await.expect("the stream is finalized");

    let removed_rows = engine
        .state()
        .catalog
        .commit_snapshot(&events(), &SnapshotChange::Truncate)
        .await
        .expect("the table is truncated");
    assert_eq!(removed_rows, 5);
    let overwritten: Vec<Event> = engine
        .db
        .fluent()
        .query("SELECT 200 AS id, 2000 AS amount")
        .dangerously_overwrite_destination_table(SHOP.table(EVENTS))
        .obj()
        .query()
        .await
        .expect("the result overwrites the table");
    assert_eq!(overwritten, rows(200..201));

    engine
        .db
        .commit_write_streams(vec![pending])
        .await
        .expect("the stream commits");
    assert_eq!(
        read_events(&engine).await,
        [rows(100..104), rows(200..201)].concat()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_truncate_on_a_cached_empty_table_removes_the_rows_another_engine_appended() {
    let first = TestEngine::start().await;
    create_events(&first).await;
    let second = first
        .open_on_same_root(|mut config| {
            config.catalog_cache_ttl = config.pointer_min_age / 2;
            config
        })
        .await;
    second
        .state()
        .catalog
        .load(&events())
        .await
        .expect("the empty table loads into the cache");
    insert(&first, 0..10).await;

    let removed_rows = second
        .state()
        .catalog
        .commit_snapshot(&events(), &SnapshotChange::Truncate)
        .await
        .expect("the table is truncated");

    assert_eq!(removed_rows, 10);
    assert_eq!(count_and_sum(&first).await, (0, None));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_built_before_the_table_lock_is_stamped_when_it_commits() {
    let engine = TestEngine::start().await;
    create_events(&engine).await;
    insert(&engine, 0..10).await;
    let mut held = engine.state().catalog.hooks().hold(CommitPoint::Loaded);
    let catalog = engine.state().catalog.clone();
    let truncate = tokio::spawn(async move {
        catalog
            .commit_snapshot(&events(), &SnapshotChange::Truncate)
            .await
    });
    held.reached().await;
    // The build happened before the hold, so the clock must move past its millisecond.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let released_at = now_ms();
    held.release();

    truncate
        .await
        .expect("the truncate task ends")
        .expect("the truncate lands");

    let loaded = reload(&engine).await;
    let snapshot = loaded
        .metadata()
        .current_snapshot()
        .expect("the truncate made a snapshot");
    assert_eq!(snapshot.summary().operation, Operation::Delete);
    assert!(
        snapshot.timestamp_ms() >= released_at,
        "stamped at {}, released at {released_at}",
        snapshot.timestamp_ms()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_stamped_more_than_a_minute_before_the_last_commit_loses_its_attempt() {
    let engine = TestEngine::start().await;
    create_events(&engine).await;
    insert(&engine, 0..10).await;
    let loaded = reload(&engine).await;
    let built = SnapshotChange::Truncate
        .build(&events(), &loaded, 8192)
        .await
        .expect("the snapshot builds");
    let last_commit = loaded.metadata().last_updated_ms();

    assert!(matches!(
        built.commit_at(loaded.metadata(), last_commit - 60_001),
        Err(Lost::ClockBehind)
    ));
    assert!(built
        .commit_at(loaded.metadata(), last_commit - 60_000)
        .is_ok());
    engine.state().catalog.discard(&built.written).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_merge_drops_a_manifest_without_live_entries_beside_an_oversized_one() {
    #[derive(Debug, Serialize)]
    struct Visit {
        id: i64,
        day: BigQueryDate,
    }
    let engine = TestEngine::with_config(|mut config| {
        config.maintenance.manifest_merge_target_entries = 2;
        config
    })
    .await;
    engine.create_dataset("shop", "EU").await;
    let visits_id = BigQueryTableId::from_static("visits");
    engine
        .db
        .fluent()
        .schema()
        .table(SHOP.table(visits_id.clone()))
        .columns(|columns| {
            columns.fields([columns.field("id").int64(), columns.field("day").date()])
        })
        .partition_by_day("day")
        .sync()
        .await
        .expect("the table is created");
    let visits = table_ref("shop", "visits");
    let insert_days = |days: Range<i8>| {
        let rows: Vec<Visit> = days
            .map(|day| Visit {
                id: i64::from(day),
                day: BigQueryDate(jiff::civil::date(2026, 10, day)),
            })
            .collect();
        let db = &engine.db;
        let table = SHOP.table(visits_id.clone());
        async move {
            db.fluent()
                .insert()
                .into(table)
                .objects(&rows)
                .execute()
                .await
                .expect("the rows are written");
        }
    };
    insert_days(1..2).await;
    engine
        .state()
        .catalog
        .commit_snapshot(&visits, &SnapshotChange::Truncate)
        .await
        .expect("the table is truncated");
    insert_days(2..5).await;
    let before = engine
        .state()
        .catalog
        .reload(&visits)
        .await
        .expect("the table loads");
    let live_per_manifest: Vec<usize> = data_manifests(&before)
        .await
        .iter()
        .map(|manifest| {
            manifest
                .entries()
                .iter()
                .filter(|entry| entry.is_alive())
                .count()
        })
        .collect();
    assert_eq!(
        live_per_manifest.iter().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([0, 3]),
        "the truncate's manifest beside the three-file append's"
    );

    engine
        .state()
        .catalog
        .commit_snapshot(&visits, &SnapshotChange::MergeManifests)
        .await
        .expect("the manifests merge");

    let after = engine
        .state()
        .catalog
        .reload(&visits)
        .await
        .expect("the table loads");
    let snapshot = after
        .metadata()
        .current_snapshot()
        .expect("the table has a snapshot");
    assert_eq!(snapshot.summary().operation, Operation::Replace);
    let entries_per_manifest: Vec<usize> = data_manifests(&after)
        .await
        .iter()
        .map(|manifest| manifest.entries().len())
        .collect();
    assert_eq!(entries_per_manifest, [2, 1]);
}
