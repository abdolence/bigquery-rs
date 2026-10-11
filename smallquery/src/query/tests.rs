//! Queries and jobs through the `bigquery` client and gcloud-sdk's generated job client: result
//! tables, the output step, dry runs, cancellation and the location rules.

use super::ANONYMOUS_DATASET;
use crate::catalog::CommitPoint;
use crate::testkit::{table_ref, TestEngine, PROJECT};
use bigquery::arrow_schema::{DataType, Field};
use bigquery::errors::BigQueryError;
use bigquery::*;
use futures::StreamExt;
use gcloud_sdk::google::cloud::bigquery::v2;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tonic::Code;

const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
const ITEMS: BigQueryTableId = BigQueryTableId::from_static("items");
const COPIES: BigQueryTableId = BigQueryTableId::from_static("copies");
const EU: BigQueryLocation = BigQueryLocation::from_static("EU");
const US: BigQueryLocation = BigQueryLocation::from_static("US");

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Place {
    city: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Item {
    id: i64,
    label: String,
    place: Option<Place>,
    tags: Vec<String>,
}

/// `shop.items` in `EU` with `count` rows, written in one request.
async fn items(engine: &TestEngine, count: i64) -> Vec<Item> {
    engine.create_dataset("shop", "EU").await;
    engine
        .db
        .fluent()
        .schema()
        .table(SHOP.table(ITEMS))
        .columns(|columns| {
            columns.fields([
                columns.field("id").int64().required(),
                columns.field("label").string().required(),
                columns
                    .field("place")
                    .record(|place| place.fields([place.field("city").string()])),
                columns.field("tags").string().repeated(),
            ])
        })
        .sync()
        .await
        .expect("the table is created");
    let rows: Vec<Item> = (0..count)
        .map(|id| Item {
            id,
            // Wide enough that a large result does not fit the inline limit.
            label: format!("{id:064}"),
            place: (id % 2 == 0).then(|| Place {
                city: Some(format!("city-{}", id % 7)),
            }),
            tags: (0..id % 3).map(|tag| format!("t{tag}")).collect(),
        })
        .collect();
    if count > 0 {
        engine
            .db
            .fluent()
            .insert()
            .into(SHOP.table(ITEMS))
            .objects(&rows)
            .execute()
            .await
            .expect("the rows are written");
    }
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_result_goes_through_a_result_table_and_storage_read() {
    let engine = TestEngine::start().await;
    let written = items(&engine, 200_000).await;
    let (mut read, stats): (Vec<Item>, BigQueryJobStats) = engine
        .db
        .fluent()
        .query("SELECT * FROM shop.items")
        .obj()
        .query_with_stats()
        .await
        .expect("the query runs");
    read.sort_by_key(|item| item.id);
    assert_eq!(read.len(), written.len());
    assert!(
        read == written,
        "the rows read back differ from the rows written"
    );
    assert_eq!(stats.total_rows, Some(200_000));
    let job = stats
        .job
        .expect("a result too large to answer inline has a job");
    let result_table = table_ref(ANONYMOUS_DATASET, job.job_id.as_str());
    let tasks = engine.plan_files(&result_table, None).await;
    assert!(!tasks.is_empty(), "the result table holds the rows");
    engine.assert_no_path_twice(&result_table).await;
    engine
        .assert_no_path_twice(&table_ref("shop", "items"))
        .await;
}

/// The fields of `field` and every field nested in it.
fn every_field(field: &Field) -> Vec<&Field> {
    let mut fields = vec![field];
    match field.data_type() {
        DataType::Struct(children) => {
            for child in children {
                fields.extend(every_field(child));
            }
        }
        DataType::List(item) => fields.extend(every_field(item)),
        _ => {}
    }
    fields
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn results_carry_bigquery_names_and_wire_types_only() {
    let engine = TestEngine::start().await;
    items(&engine, 5).await;
    let mut batches = engine
        .db
        .fluent()
        .query(
            "SELECT id, id * 2, ROW_NUMBER() OVER (ORDER BY id), place.city, place, tags, \
             COUNT(*) OVER () AS total FROM shop.items",
        )
        .record_batches()
        .await
        .expect("the query runs");
    let batch = batches
        .next()
        .await
        .expect("the result has a batch")
        .expect("the batch decodes");
    let schema = batch.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        ["id", "f0_", "f1_", "city", "place", "tags", "total"]
    );
    let types: Vec<&DataType> = schema.fields().iter().map(|f| f.data_type()).collect();
    assert_eq!(
        types[..4],
        [
            &DataType::Int64,
            &DataType::Int64,
            &DataType::Int64,
            &DataType::Utf8
        ]
    );
    for field in schema.fields() {
        for nested in every_field(field) {
            assert!(
                !nested.metadata().contains_key("PARQUET:field_id"),
                "{} carries an Iceberg field id",
                nested.name()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dry_run_reports_the_schema_and_the_bytes_scanned_without_a_job() {
    let engine = TestEngine::start().await;
    items(&engine, 100).await;
    let dry_run = engine
        .db
        .fluent()
        .query("SELECT id, label FROM shop.items WHERE id < 10")
        .dry_run()
        .await
        .expect("the dry run plans");
    let planned: u64 = engine
        .plan_files(&table_ref("shop", "items"), None)
        .await
        .iter()
        .map(|task| task.length)
        .sum();
    assert_eq!(
        dry_run.total_bytes_processed,
        Some(i64::try_from(planned).expect("fits"))
    );
    let schema = dry_run.schema.expect("a dry run reports the schema");
    let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, ["id", "label"]);
    assert!(engine
        .state()
        .queries
        .jobs
        .list(&PROJECT.parse().expect("a valid project"))
        .is_empty());
}

/// A statement that runs long enough to be cancelled: a three-way cross join of `items`.
const LONG_QUERY: &str =
    "SELECT SUM(a.id + b.id + c.id) AS total FROM shop.items a, shop.items b, shop.items c";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_job_stops_and_reports_it() {
    let engine = TestEngine::start().await;
    items(&engine, 2_000).await;
    let response = engine
        .db
        .job_client()
        .query(v2::PostQueryRequest {
            project_id: PROJECT.to_string(),
            query_request: Some(v2::QueryRequest {
                query: LONG_QUERY.to_string(),
                use_legacy_sql: Some(false),
                timeout_ms: Some(10),
                job_creation_mode: v2::query_request::JobCreationMode::JobCreationRequired.into(),
                ..Default::default()
            }),
        })
        .await
        .expect("the query starts")
        .into_inner();
    assert_eq!(response.job_complete, Some(false));
    let job = BigQueryJobRef::from(response.job_reference.expect("a running query has a job"));
    engine
        .db
        .cancel_job(&job)
        .await
        .expect("the job is cancelled");
    let details = engine
        .db
        .job_client()
        .get_job(v2::GetJobRequest {
            project_id: PROJECT.to_string(),
            job_id: job.job_id.to_string(),
            location: "EU".to_string(),
        })
        .await
        .expect("the job is read")
        .into_inner();
    let status = details.status.expect("a job has a status");
    assert_eq!(status.state, "DONE");
    assert_eq!(
        status.error_result.map(|error| error.reason),
        Some("stopped".to_string())
    );
    let again = engine.db.cancel_job(&job).await;
    assert!(again.is_ok(), "a finished job stays finished: {again:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_job_runs_where_its_datasets_are() {
    let engine = TestEngine::start().await;
    items(&engine, 3).await;
    let elsewhere = engine
        .db
        .fluent()
        .query("SELECT COUNT(*) AS n FROM shop.items")
        .location(US)
        .execute()
        .await;
    assert!(
        matches!(elsewhere, Err(BigQueryError::DataNotFoundError(_))),
        "{elsewhere:?}"
    );

    #[derive(Debug, Deserialize, PartialEq)]
    struct Count {
        n: i64,
    }
    let counted: Vec<Count> = engine
        .db
        .fluent()
        .query("SELECT COUNT(*) AS n FROM shop.items")
        .location(EU)
        .obj()
        .query()
        .await
        .expect("the query runs where its dataset is");
    assert_eq!(counted, [Count { n: 3 }]);

    for location in ["US", "asia-northeast1", "EU"] {
        let response = engine
            .db
            .job_client()
            .query(v2::PostQueryRequest {
                project_id: PROJECT.to_string(),
                query_request: Some(v2::QueryRequest {
                    query: "SELECT 1 AS one".to_string(),
                    use_legacy_sql: Some(false),
                    location: location.to_string(),
                    ..Default::default()
                }),
            })
            .await
            .expect("a query of no dataset runs anywhere")
            .into_inner();
        assert_eq!(response.location, location);
        assert_eq!(response.total_rows, Some(1));
    }

    let (_, stats): (Vec<Count>, BigQueryJobStats) = engine
        .db
        .fluent()
        .query("SELECT COUNT(*) AS n FROM shop.items")
        .job_creation_required()
        .obj()
        .query_with_stats()
        .await
        .expect("the query runs");
    let job = stats.job.expect("the query was required to make a job");
    assert_eq!(job.location, Some(EU));
    let wrong = engine
        .db
        .job_client()
        .get_job(v2::GetJobRequest {
            project_id: PROJECT.to_string(),
            job_id: job.job_id.to_string(),
            location: "US".to_string(),
        })
        .await
        .expect_err("a job is not found in another location");
    assert_eq!(wrong.code(), Code::NotFound);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_job_appends_its_result_to_a_destination_table() {
    let engine = TestEngine::start().await;
    items(&engine, 20).await;
    #[derive(Debug, Deserialize, PartialEq)]
    struct Copy {
        id: i64,
        label: String,
    }
    let first: Vec<Copy> = engine
        .db
        .fluent()
        .query("SELECT id, label FROM shop.items WHERE id < 5")
        .destination_table(SHOP.table(COPIES))
        .obj()
        .query()
        .await
        .expect("the result is written to a new table");
    assert_eq!(first.len(), 5);
    let appended: Vec<Copy> = engine
        .db
        .fluent()
        .query("SELECT id, label FROM shop.items WHERE id >= 15")
        .append_to_destination_table(SHOP.table(COPIES))
        .obj()
        .query()
        .await
        .expect("the result is appended");
    assert_eq!(appended.len(), 10);
    let refused = engine
        .db
        .fluent()
        .query("SELECT id, label FROM shop.items")
        .destination_table(SHOP.table(COPIES))
        .execute()
        .await;
    assert!(
        refused.is_err(),
        "a table with rows is not empty: {refused:?}"
    );
    engine
        .assert_no_path_twice(&table_ref("shop", "copies"))
        .await;
}

const COUNTS: BigQueryTableId = BigQueryTableId::from_static("counts");

/// The job `job_id` of the test project, as the engine holds it.
fn engine_job(engine: &TestEngine, job_id: &str) -> std::sync::Arc<super::jobs::Job> {
    engine
        .state()
        .queries
        .jobs
        .get(
            &PROJECT.parse().expect("a valid project"),
            &job_id.parse().expect("a valid job ID"),
            None,
        )
        .expect("the job is registered")
}

/// The job's status once it finished.
async fn finished_status(engine: &TestEngine, job_id: &str) -> v2::JobStatus {
    let job = engine_job(engine, job_id);
    job.wait(std::time::Duration::from_secs(60)).await;
    let status = job.v2().status.expect("a job has a status");
    assert_eq!(status.state, "DONE", "{job_id} finished");
    status
}

/// Starts a query job that writes the result of `sql` into `shop.<table>` under
/// `write_disposition`, with `job_timeout_ms` when set, and returns its ID.
async fn insert_query_job(
    engine: &TestEngine,
    sql: &str,
    table: &str,
    write_disposition: &str,
    job_timeout_ms: Option<i64>,
) -> String {
    let job = engine
        .db
        .job_client()
        .insert_job(v2::InsertJobRequest {
            project_id: PROJECT.to_string(),
            job: Some(v2::Job {
                configuration: Some(v2::JobConfiguration {
                    job_type: "QUERY".to_string(),
                    query: Some(v2::JobConfigurationQuery {
                        query: sql.to_string(),
                        use_legacy_sql: Some(false),
                        destination_table: Some(v2::TableReference {
                            project_id: PROJECT.to_string(),
                            dataset_id: "shop".to_string(),
                            table_id: table.to_string(),
                        }),
                        write_disposition: write_disposition.to_string(),
                        ..Default::default()
                    }),
                    job_timeout_ms,
                    ..Default::default()
                }),
                ..Default::default()
            }),
        })
        .await
        .expect("the job is inserted")
        .into_inner();
    job.job_reference.expect("a job has a reference").job_id
}

/// `shop.copies`, empty, with the columns of `SELECT id, label FROM shop.items`.
async fn empty_copies(engine: &TestEngine) {
    engine
        .db
        .fluent()
        .schema()
        .table(SHOP.table(COPIES))
        .columns(|columns| {
            columns.fields([columns.field("id").int64(), columns.field("label").string()])
        })
        .sync()
        .await
        .expect("the table is created");
}

/// The rows of `shop.<table>`.
async fn count_rows(engine: &TestEngine, table: &str) -> i64 {
    #[derive(Debug, Deserialize)]
    struct Count {
        n: i64,
    }
    let counted: Vec<Count> = engine
        .db
        .fluent()
        .query(format!("SELECT COUNT(*) AS n FROM shop.{table}"))
        .obj()
        .query()
        .await
        .expect("the table counts");
    counted[0].n
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parameter_selected_without_an_alias_is_anonymous() {
    let engine = TestEngine::start().await;
    let mut batches = engine
        .db
        .fluent()
        .query("SELECT @limit, @limit + 1 AS next")
        .param("limit", 7i64)
        .record_batches()
        .await
        .expect("the query runs");
    let batch = batches
        .next()
        .await
        .expect("the result has a batch")
        .expect("the batch decodes");
    let schema = batch.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["f0_", "next"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_value_its_destination_column_cannot_hold_exactly_fails_the_job() {
    let engine = TestEngine::start().await;
    engine.create_dataset("shop", "EU").await;
    engine
        .db
        .fluent()
        .schema()
        .table(SHOP.table(COUNTS))
        .columns(|columns| {
            columns.fields([columns.field("n").int64(), columns.field("label").string()])
        })
        .sync()
        .await
        .expect("the table is created");
    for sql in ["SELECT 2.7 AS n", "SELECT 12 AS label"] {
        let job = insert_query_job(&engine, sql, "counts", "WRITE_APPEND", None).await;
        let status = finished_status(&engine, &job).await;
        assert_eq!(
            status.error_result.map(|error| error.reason).as_deref(),
            Some("invalidQuery"),
            "{sql}"
        );
    }
    assert_eq!(count_rows(&engine, "counts").await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_job_cancelled_while_its_commit_runs_finishes_from_the_commit() {
    let engine = TestEngine::start().await;
    items(&engine, 20).await;
    empty_copies(&engine).await;
    let mut held = engine.state().catalog.hooks().hold(CommitPoint::Loaded);
    let job = insert_query_job(
        &engine,
        "SELECT id, label FROM shop.items",
        "copies",
        "WRITE_APPEND",
        None,
    )
    .await;
    held.reached().await;
    engine
        .db
        .job_client()
        .cancel_job(v2::CancelJobRequest {
            project_id: PROJECT.to_string(),
            job_id: job.clone(),
            location: String::new(),
        })
        .await
        .expect("the cancel is answered");
    held.release();
    let status = finished_status(&engine, &job).await;
    assert_eq!(status.error_result, None);
    assert_eq!(count_rows(&engine, "copies").await, 20);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_job_whose_timeout_passes_while_its_commit_runs_finishes_from_the_commit() {
    let engine = TestEngine::start().await;
    items(&engine, 20).await;
    empty_copies(&engine).await;
    let mut held = engine.state().catalog.hooks().hold(CommitPoint::Loaded);
    let timeout = std::time::Duration::from_secs(2);
    let job = insert_query_job(
        &engine,
        "SELECT id, label FROM shop.items",
        "copies",
        "WRITE_APPEND",
        Some(i64::try_from(timeout.as_millis()).expect("fits")),
    )
    .await;
    held.reached().await;
    // Held past the job's timeout, which no longer applies once the commit is queued.
    tokio::time::sleep(timeout * 2).await;
    held.release();
    let status = finished_status(&engine, &job).await;
    assert_eq!(status.error_result, None);
    assert_eq!(count_rows(&engine, "copies").await, 20);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn of_two_write_empty_jobs_into_one_empty_table_only_one_appends() {
    let engine = TestEngine::start().await;
    items(&engine, 20).await;
    empty_copies(&engine).await;
    let groups = engine.state().writes.groups().clone();
    let joined_before = groups.joined();
    let mut held = engine.state().catalog.hooks().hold(CommitPoint::Loaded);
    let sql = "SELECT id, label FROM shop.items";
    let first = insert_query_job(&engine, sql, "copies", "WRITE_EMPTY", None).await;
    held.reached().await;
    let second = insert_query_job(&engine, sql, "copies", "WRITE_EMPTY", None).await;
    while groups.joined() < joined_before + 2 {
        tokio::task::yield_now().await;
    }
    held.release();
    let mut reasons = Vec::new();
    for job in [first, second] {
        let status = finished_status(&engine, &job).await;
        reasons.push(status.error_result.map(|error| error.reason));
    }
    reasons.sort();
    assert_eq!(reasons, [None, Some("duplicate".to_string())]);
    assert_eq!(count_rows(&engine, "copies").await, 20);
}

/// Waits until `table` has no pointer left, failing after `deadline`.
async fn assert_dropped_within(engine: &TestEngine, table: &crate::ids::TableRef) {
    let deadline = Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let gone = matches!(
            engine.state().catalog.reload(table).await,
            Err(crate::error::EngineError::NotFound { .. })
        );
        if gone && engine.table_directories(table).is_empty() {
            return;
        }
        assert!(Instant::now() < deadline, "{table} is still there");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_job_s_result_table_is_dropped() {
    let engine = TestEngine::start().await;
    let mut held = engine.state().catalog.hooks().hold(CommitPoint::Created);
    let response = engine
        .db
        .job_client()
        .query(v2::PostQueryRequest {
            project_id: PROJECT.to_string(),
            query_request: Some(v2::QueryRequest {
                query: "SELECT 1 AS one".to_string(),
                use_legacy_sql: Some(false),
                timeout_ms: Some(0),
                job_creation_mode: v2::query_request::JobCreationMode::JobCreationRequired.into(),
                ..Default::default()
            }),
        })
        .await
        .expect("the query starts")
        .into_inner();
    let job = response.job_reference.expect("a required job").job_id;
    held.reached().await;
    engine
        .db
        .job_client()
        .cancel_job(v2::CancelJobRequest {
            project_id: PROJECT.to_string(),
            job_id: job.clone(),
            location: String::new(),
        })
        .await
        .expect("the cancel is answered");
    held.release();
    engine
        .db
        .job_client()
        .delete_job(v2::DeleteJobRequest {
            project_id: PROJECT.to_string(),
            job_id: job.clone(),
            location: String::new(),
        })
        .await
        .expect("the job is deleted");
    assert_dropped_within(&engine, &table_ref(ANONYMOUS_DATASET, &job)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_engine_opening_a_root_drops_result_tables_older_than_the_job_ttl() {
    let engine = TestEngine::start().await;
    let (_, stats): (Vec<serde_json::Value>, BigQueryJobStats) = engine
        .db
        .fluent()
        .query("SELECT 1 AS one")
        .job_creation_required()
        .obj()
        .query_with_stats()
        .await
        .expect("the query runs");
    let job = stats.job.expect("a required job");
    let table = table_ref(ANONYMOUS_DATASET, job.job_id.as_str());
    assert!(engine.state().catalog.reload(&table).await.is_ok());
    let reopened = engine
        .open_on_same_root(|mut config| {
            config.job_ttl = std::time::Duration::ZERO;
            config
        })
        .await;
    assert_dropped_within(&reopened, &table).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_result_above_the_inline_limit_is_never_held_whole() {
    let limit = crate::ByteSize::from_bytes(64 * 1024);
    let engine = TestEngine::with_config(|mut config| {
        config.inline_result_limit = limit;
        config
    })
    .await;
    let written = items(&engine, 20_000).await;
    let (read, stats): (Vec<Item>, BigQueryJobStats) = engine
        .db
        .fluent()
        .query("SELECT * FROM shop.items")
        .obj()
        .query_with_stats()
        .await
        .expect("the query runs");
    let peak = engine
        .state()
        .queries
        .inline_peak
        .load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        u64::try_from(peak).expect("fits") <= limit.bytes(),
        "{peak} bytes held for an inline answer"
    );
    assert!(stats.job.is_some(), "a result above the limit has a job");
    assert_eq!(read.len(), written.len());
}
