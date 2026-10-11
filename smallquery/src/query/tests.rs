//! Queries and jobs through the `bigquery` client and gcloud-sdk's generated job client: result
//! tables, the output step, dry runs, cancellation and the location rules.

use super::ANONYMOUS_DATASET;
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
    let started = Instant::now();
    let (mut read, stats): (Vec<Item>, BigQueryJobStats) = engine
        .db
        .fluent()
        .query("SELECT * FROM shop.items")
        .obj()
        .query_with_stats()
        .await
        .expect("the query runs");
    let elapsed = started.elapsed();
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
    eprintln!(
        "200,000 rows queried through a result table and Storage Read in {} ms",
        elapsed.as_millis()
    );
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
