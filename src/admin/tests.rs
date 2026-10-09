//! The plain dataset, table and job calls against the fake server: the requests they send and
//! the typed values they return.

use crate::db::fake::table::v2_field;
use crate::db::fake::{FakeBigQuery, FakeCall};
use crate::errors::BigQueryError;
use crate::testing::{BigQueryFake, BigQueryFakeCode, BigQueryFakeFault, BigQueryFakeRpc};
use crate::{BigQueryDataset, BigQueryDatasetSummary, BigQueryJob, BigQueryTable};
use crate::{
    BigQueryDatasetId, BigQueryDatasetRef, BigQueryFieldMode, BigQueryFieldType, BigQueryJobRef,
    BigQueryJobState, BigQueryJobType, BigQueryListJobsParams, BigQueryPartitionUnit,
    BigQueryPartitioning, BigQueryStatementType, BigQueryTableId, BigQueryTableType,
};
use crate::{BigQueryJobId, BigQueryLabels, BigQueryLocation};
use crate::{BigQueryResult, BigQueryTableSummary, BigQueryTableSupport};
use futures::StreamExt;
use gcloud_sdk::google::cloud::bigquery::v2;
use gcloud_sdk::tonic::Code;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");

/// 2026-10-04T00:00:00Z in milliseconds.
const CREATED_MS: i64 = 1_791_072_000_000;

fn labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

fn sorted(map: &HashMap<String, String>) -> BTreeMap<String, String> {
    map.clone().into_iter().collect()
}

fn dataset_body(project: &str, dataset: &str) -> v2::Dataset {
    v2::Dataset {
        etag: "e0".into(),
        dataset_reference: Some(v2::DatasetReference {
            dataset_id: dataset.into(),
            project_id: project.into(),
        }),
        location: "EU".into(),
        description: Some("Shop".into()),
        labels: labels(&[("team", "shop")]),
        default_table_expiration_ms: Some(3_600_000),
        creation_time: CREATED_MS,
        last_modified_time: CREATED_MS + 1,
        ..Default::default()
    }
}

async fn unexpected(call: FakeCall) {
    let message = format!("unexpected {}", call.method());
    call.fail(Code::Unimplemented, &message);
}

#[tokio::test]
async fn get_reads_another_project_into_a_typed_dataset() {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        if call.method() != "GetDataset" {
            return unexpected(call).await;
        }
        let request: v2::GetDatasetRequest = call.next_request().await.expect("a request");
        call.log(format!(
            "GetDataset {}.{}",
            request.project_id, request.dataset_id
        ));
        call.reply(&v2::Dataset {
            description: Some(String::new()),
            ..dataset_body(&request.project_id, &request.dataset_id)
        });
    })
    .await;
    let dataset = fake
        .db
        .fluent()
        .schema()
        .dataset(BigQueryDatasetRef::new("other", SHOP).expect("a valid project"))
        .get()
        .await
        .expect("the call succeeds");
    assert_eq!(fake.calls(), ["GetDataset other.shop"]);
    assert_eq!(dataset.reference.project(), Some("other"));
    assert_eq!(dataset.description, None, "an empty description is unset");
    assert_eq!(dataset.labels, BigQueryLabels::from([("team", "shop")]));
    assert_eq!(
        dataset.default_table_expiration,
        Some(Duration::from_secs(3600))
    );
    assert_eq!(dataset.default_partition_expiration, None);
    assert_eq!(
        dataset.last_modified_time,
        Some(jiff::Timestamp::from_millisecond(CREATED_MS + 1).expect("a timestamp"))
    );
}

/// A dataset as `GetDataset` returns it, labelled `a` and `b`.
fn read_dataset() -> v2::Dataset {
    v2::Dataset {
        labels: labels(&[("a", "1"), ("b", "2")]),
        ..dataset_body("fake-project", "shop")
    }
}

#[tokio::test]
async fn update_writes_the_read_dataset_back_with_its_etag_and_the_changes() {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        match call.method() {
            "GetDataset" => {
                let _: v2::GetDatasetRequest = call.next_request().await.expect("a request");
                call.log("GetDataset");
                call.reply(&read_dataset());
            }
            "UpdateDataset" => {
                let request: v2::UpdateOrPatchDatasetRequest =
                    call.next_request().await.expect("a request");
                let precondition = call.header("if-match").unwrap_or_else(|| "none".into());
                let body = request.dataset.clone().unwrap_or_default();
                call.log(format!(
                    "UpdateDataset {}.{} if-match={precondition} mode={:?} description={:?} \
                     labels={:?} expiration={:?}",
                    request.project_id,
                    request.dataset_id,
                    request.update_mode(),
                    body.description,
                    sorted(&body.labels),
                    body.default_table_expiration_ms,
                ));
                call.reply(&body);
            }
            _ => unexpected(call).await,
        }
    })
    .await;
    let updated = fake
        .db
        .fluent()
        .schema()
        .dataset(SHOP)
        .update()
        .remove_label("a")
        .label("c", "3")
        .description("new")
        .execute()
        .await
        .expect("the call succeeds");
    assert_eq!(
        fake.calls(),
        [
            "GetDataset",
            r#"UpdateDataset fake-project.shop if-match=e0 mode=UpdateMetadata description=Some("new") labels={"b": "2", "c": "3"} expiration=Some(3600000)"#,
        ]
    );
    assert_eq!(updated.description.as_deref(), Some("new"));
}

fn listed_dataset(dataset: &str) -> v2::ListFormatDataset {
    v2::ListFormatDataset {
        dataset_reference: Some(v2::DatasetReference {
            dataset_id: dataset.into(),
            project_id: "acme-prod".into(),
        }),
        location: "US".into(),
        labels: labels(&[("team", dataset)]),
        ..Default::default()
    }
}

/// `ListDatasets` in two pages, `shop` then `warehouse`; with `fail_second`, the second page fails.
async fn two_dataset_pages(fail_second: bool) -> FakeBigQuery {
    FakeBigQuery::start(move |mut call: FakeCall| async move {
        if call.method() != "ListDatasets" {
            return unexpected(call).await;
        }
        let request: v2::ListDatasetsRequest = call.next_request().await.expect("a request");
        call.log(format!(
            "ListDatasets {} max_results={:?} page_token={:?}",
            request.project_id, request.max_results, request.page_token
        ));
        match request.page_token.as_str() {
            "" => call.reply(&v2::DatasetList {
                datasets: vec![listed_dataset("shop")],
                next_page_token: "p2".into(),
                ..Default::default()
            }),
            _ if fail_second => call.fail(Code::InvalidArgument, "bad page token"),
            _ => call.reply(&v2::DatasetList {
                datasets: vec![listed_dataset("warehouse")],
                ..Default::default()
            }),
        }
    })
    .await
}

#[tokio::test]
async fn the_dataset_listing_follows_page_tokens() {
    let fake = two_dataset_pages(false).await;
    let datasets: Vec<_> = fake
        .db
        .fluent()
        .schema()
        .datasets()
        .project("acme-prod")
        .page_size(1)
        .stream_all()
        .await
        .expect("the call succeeds")
        .collect()
        .await;
    assert_eq!(
        fake.calls(),
        [
            r#"ListDatasets acme-prod max_results=Some(1) page_token="""#,
            r#"ListDatasets acme-prod max_results=Some(1) page_token="p2""#,
        ]
    );
    let references: Vec<_> = datasets
        .iter()
        .map(|dataset| dataset.reference.clone())
        .collect();
    assert_eq!(
        references,
        [
            BigQueryDatasetRef::new("acme-prod", SHOP).expect("a valid project"),
            BigQueryDatasetRef::new("acme-prod", BigQueryDatasetId::from_static("warehouse"))
                .expect("a valid project"),
        ]
    );
    assert_eq!(
        datasets[1].location,
        Some(BigQueryLocation::from_static("US"))
    );
}

#[tokio::test]
async fn a_failed_listing_page_ends_the_listing() {
    let fake = two_dataset_pages(true).await;
    let with_errors: Vec<_> = fake
        .db
        .fluent()
        .schema()
        .datasets()
        .stream_all_with_errors()
        .await
        .expect("the call succeeds")
        .collect()
        .await;
    assert_eq!(with_errors.len(), 2, "{with_errors:?}");
    assert!(with_errors[0].is_ok());
    assert!(matches!(
        with_errors[1],
        Err(BigQueryError::DatabaseError(_))
    ));
    let logged: Vec<_> = fake
        .db
        .fluent()
        .schema()
        .datasets()
        .stream_all()
        .await
        .expect("the call succeeds")
        .collect()
        .await;
    assert_eq!(logged.len(), 1);
}

#[tokio::test]
async fn get_table_normalises_legacy_types_and_reads_views_too() {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        if call.method() != "GetTable" {
            return unexpected(call).await;
        }
        let request: v2::GetTableRequest = call.next_request().await.expect("a request");
        call.log(format!(
            "GetTable {}.{}.{}",
            request.project_id, request.dataset_id, request.table_id
        ));
        let reference = v2::TableReference {
            project_id: request.project_id.clone(),
            dataset_id: request.dataset_id.clone(),
            table_id: request.table_id.clone(),
        };
        let view = request.table_id == "orders_view";
        call.reply(&v2::Table {
            table_reference: Some(reference),
            r#type: if view { "VIEW" } else { "TABLE" }.into(),
            schema: Some(v2::TableSchema {
                fields: vec![
                    v2_field("id", "INTEGER", "REQUIRED"),
                    v2_field("total", "FLOAT", ""),
                    v2_field("placed_at", "TIMESTAMP", "NULLABLE"),
                ],
                ..Default::default()
            }),
            time_partitioning: (!view).then(|| v2::TimePartitioning {
                r#type: "DAY".into(),
                field: Some("placed_at".into()),
                ..Default::default()
            }),
            clustering: Some(v2::Clustering {
                fields: vec!["id".into()],
            }),
            num_rows: Some(7),
            num_bytes: Some(224),
            location: "EU".into(),
            creation_time: CREATED_MS,
            ..Default::default()
        });
    })
    .await;
    let table = fake
        .db
        .fluent()
        .schema()
        .table(SHOP.table(ORDERS))
        .get()
        .await
        .expect("the call succeeds");
    assert_eq!(fake.calls(), ["GetTable fake-project.shop.orders"]);
    assert_eq!(
        table.reference,
        BigQueryDatasetRef::new("fake-project", SHOP)
            .expect("a valid project")
            .table(ORDERS)
    );
    assert_eq!(table.table_type, Some(BigQueryTableType::Table));
    let columns: Vec<_> = table
        .schema
        .fields
        .iter()
        .map(|field| (field.name.as_str(), field.field_type.clone(), field.mode))
        .collect();
    assert_eq!(
        columns,
        [
            ("id", BigQueryFieldType::Int64, BigQueryFieldMode::Required),
            (
                "total",
                BigQueryFieldType::Float64,
                BigQueryFieldMode::Nullable
            ),
            (
                "placed_at",
                BigQueryFieldType::Timestamp,
                BigQueryFieldMode::Nullable
            ),
        ]
    );
    assert_eq!(
        table.partitioning,
        Some(BigQueryPartitioning::Time {
            unit: BigQueryPartitionUnit::Day,
            column: Some("placed_at".into()),
        })
    );
    assert_eq!(table.clustering, ["id"]);
    assert_eq!((table.num_rows, table.num_bytes), (Some(7), Some(224)));

    let view = fake
        .db
        .fluent()
        .schema()
        .table(SHOP.table(BigQueryTableId::from_static("orders_view")))
        .get()
        .await
        .expect("the call succeeds");
    assert_eq!(view.table_type, Some(BigQueryTableType::View));
}

#[tokio::test]
async fn table_delete_and_listing_name_their_dataset() {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        match call.method() {
            "DeleteTable" => {
                let request: v2::DeleteTableRequest = call.next_request().await.expect("a request");
                call.log(format!(
                    "DeleteTable {}.{}.{}",
                    request.project_id, request.dataset_id, request.table_id
                ));
                call.reply(&());
            }
            "ListTables" => {
                let request: v2::ListTablesRequest = call.next_request().await.expect("a request");
                call.log(format!(
                    "ListTables {}.{} max_results={:?}",
                    request.project_id, request.dataset_id, request.max_results
                ));
                call.reply(&v2::TableList {
                    tables: vec![v2::ListFormatTable {
                        table_reference: Some(v2::TableReference {
                            project_id: request.project_id,
                            dataset_id: request.dataset_id,
                            table_id: "order_totals".into(),
                        }),
                        r#type: "MATERIALIZED_VIEW".into(),
                        creation_time: CREATED_MS,
                        ..Default::default()
                    }],
                    ..Default::default()
                });
            }
            _ => unexpected(call).await,
        }
    })
    .await;
    fake.db
        .fluent()
        .schema()
        .table(SHOP.table(ORDERS))
        .delete()
        .await
        .expect("the call succeeds");
    let tables: Vec<_> = fake
        .db
        .fluent()
        .schema()
        .dataset(BigQueryDatasetRef::new("other", SHOP).expect("a valid project"))
        .tables()
        .page_size(50)
        .stream_all()
        .await
        .expect("the call succeeds")
        .collect()
        .await;
    assert_eq!(
        fake.calls(),
        [
            "DeleteTable fake-project.shop.orders",
            "ListTables other.shop max_results=Some(50)",
        ]
    );
    assert_eq!(tables.len(), 1);
    assert_eq!(
        tables[0].reference,
        BigQueryDatasetRef::new("other", SHOP)
            .expect("a valid project")
            .table(BigQueryTableId::from_static("order_totals"))
    );
    assert_eq!(
        tables[0].table_type,
        Some(BigQueryTableType::MaterializedView)
    );
}

fn failed_query_job() -> v2::Job {
    v2::Job {
        job_reference: Some(v2::JobReference {
            project_id: "fake-project".into(),
            job_id: "job1".into(),
            location: Some("EU".into()),
        }),
        user_email: "me@example.com".into(),
        configuration: Some(v2::JobConfiguration {
            job_type: "QUERY".into(),
            ..Default::default()
        }),
        statistics: Some(v2::JobStatistics {
            creation_time: CREATED_MS,
            start_time: CREATED_MS + 10,
            end_time: CREATED_MS + 20,
            query: Some(v2::JobStatistics2 {
                total_bytes_billed: Some(0),
                statement_type: "SELECT".into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        status: Some(v2::JobStatus {
            state: "DONE".into(),
            error_result: Some(v2::ErrorProto {
                reason: "invalidQuery".into(),
                message: "Syntax error".into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn a_failed_job_reads_as_a_job_and_deletes_in_its_location() {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        match call.method() {
            "GetJob" => {
                let request: v2::GetJobRequest = call.next_request().await.expect("a request");
                call.log(format!("GetJob {} at {}", request.job_id, request.location));
                call.reply(&failed_query_job());
            }
            "DeleteJob" => {
                let request: v2::DeleteJobRequest = call.next_request().await.expect("a request");
                call.log(format!(
                    "DeleteJob {}.{} at {}",
                    request.project_id, request.job_id, request.location
                ));
                call.reply(&());
            }
            _ => unexpected(call).await,
        }
    })
    .await;
    let reference = BigQueryJobRef {
        project_id: "fake-project".into(),
        job_id: BigQueryJobId::new("job1").expect("a job ID"),
        location: Some(BigQueryLocation::from_static("EU")),
    };
    let job = fake
        .db
        .get_job(&reference)
        .await
        .expect("the call succeeds");
    fake.db
        .delete_job(&reference)
        .await
        .expect("the call succeeds");
    assert_eq!(
        fake.calls(),
        ["GetJob job1 at EU", "DeleteJob fake-project.job1 at EU"]
    );
    assert_eq!(job.reference, reference);
    assert_eq!(job.state, Some(BigQueryJobState::Done));
    assert_eq!(job.job_type, Some(BigQueryJobType::Query));
    assert_eq!(job.statement_type, Some(BigQueryStatementType::Select));
    assert_eq!(job.total_bytes_billed, Some(0));
    assert_eq!(job.user_email.as_deref(), Some("me@example.com"));
    assert_eq!(
        job.error.map(|error| error.reason).as_deref(),
        Some("invalidQuery")
    );
    assert_eq!(
        job.end_time,
        Some(jiff::Timestamp::from_millisecond(CREATED_MS + 20).expect("a timestamp"))
    );
}

#[tokio::test]
async fn the_job_listing_sends_its_filters_on_every_page() {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        if call.method() != "ListJobs" {
            return unexpected(call).await;
        }
        let request: v2::ListJobsRequest = call.next_request().await.expect("a request");
        call.log(format!(
            "ListJobs {} all_users={} min={} max={:?} states={:?} projection={:?} \
             max_results={:?} page_token={:?}",
            request.project_id,
            request.all_users,
            request.min_creation_time,
            request.max_creation_time,
            request.state_filter,
            request.projection(),
            request.max_results,
            request.page_token,
        ));
        let job = failed_query_job();
        let listed = v2::ListFormatJob {
            job_reference: job.job_reference,
            configuration: job.configuration,
            statistics: job.statistics,
            status: job.status,
            user_email: job.user_email,
            ..Default::default()
        };
        call.reply(&v2::JobList {
            jobs: vec![listed],
            next_page_token: if request.page_token.is_empty() {
                "p2".into()
            } else {
                String::new()
            },
            ..Default::default()
        });
    })
    .await;
    let since = jiff::Timestamp::from_millisecond(CREATED_MS).expect("a timestamp");
    let jobs: Vec<_> = fake
        .db
        .stream_jobs(
            BigQueryListJobsParams::new()
                .with_all_users(true)
                .with_min_creation_time(since)
                .with_states(vec![BigQueryJobState::Running, BigQueryJobState::Done])
                .with_page_size(1),
        )
        .await
        .expect("the call succeeds")
        .collect()
        .await;
    let filters = format!(
        "all_users=true min={CREATED_MS} max=None states=[2, 0] projection=Full \
         max_results=Some(1)"
    );
    assert_eq!(
        fake.calls(),
        [
            format!(r#"ListJobs fake-project {filters} page_token="""#),
            format!(r#"ListJobs fake-project {filters} page_token="p2""#),
        ]
    );
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].state, Some(BigQueryJobState::Done));
    assert_eq!(jobs[0].job_type, Some(BigQueryJobType::Query));
}

#[tokio::test]
async fn create_returns_the_stored_dataset() -> BigQueryResult<()> {
    let fake = BigQueryFake::start().await?;

    let created = fake
        .db()
        .fluent()
        .schema()
        .dataset(SHOP)
        .create()
        .location(BigQueryLocation::from_static("EU"))
        .description("Shop")
        .labels([("team", "shop")])
        .default_table_expiration(Duration::from_secs(2 * 3600))
        .execute()
        .await?;

    assert_eq!(
        created.reference,
        BigQueryDatasetRef::new("fake-project", SHOP)?
    );
    assert_eq!(created.location, Some(BigQueryLocation::from_static("EU")));
    assert_eq!(created.description.as_deref(), Some("Shop"));
    assert_eq!(created.labels, BigQueryLabels::from([("team", "shop")]));
    assert_eq!(
        created.default_table_expiration,
        Some(Duration::from_secs(2 * 3600))
    );
    Ok(())
}

#[tokio::test]
async fn a_label_replacement_and_a_cleared_description_apply_in_order() -> BigQueryResult<()> {
    let fake = BigQueryFake::start().await?;
    let shop = || fake.db().fluent().schema().dataset(SHOP);
    shop().create().description("Shop").execute().await?;

    shop()
        .update()
        .label("x", "1")
        .labels([("only", "this")])
        .label("also", "this")
        .clear_description()
        .execute()
        .await?;

    let read = shop().get().await?;
    assert_eq!(read.description, None);
    assert_eq!(
        read.labels,
        BigQueryLabels::from([("also", "this"), ("only", "this")])
    );
    Ok(())
}

#[tokio::test]
async fn an_update_refused_on_its_etag_is_a_data_conflict_sent_once() -> BigQueryResult<()> {
    let fake = BigQueryFake::start().await?;
    fake.create_dataset(SHOP)?;
    let refusal = fake
        .fault(BigQueryFakeRpc::UpdateDataset)
        .fails(BigQueryFakeFault::status(
            BigQueryFakeCode::FailedPrecondition,
            "Precondition check failed.",
        ))?;

    let result = fake
        .db()
        .fluent()
        .schema()
        .dataset(SHOP)
        .update()
        .label("c", "3")
        .execute()
        .await;

    assert!(
        matches!(result, Err(BigQueryError::DataConflictError(_))),
        "{result:?}"
    );
    assert_eq!(refusal.calls(), 1);
    Ok(())
}

#[tokio::test]
async fn an_unrecognised_state_filter_is_refused_before_any_request() -> BigQueryResult<()> {
    let fake = BigQueryFake::start().await?;
    let result = fake
        .db()
        .stream_jobs(
            BigQueryListJobsParams::new()
                .with_states(vec![BigQueryJobState::Other("PAUSED".into())]),
        )
        .await;
    assert!(
        matches!(result, Err(BigQueryError::InvalidParametersError(_))),
        "{:?}",
        result.err()
    );
    Ok(())
}

fn assert_unexpected<T: std::fmt::Debug>(what: &str, result: BigQueryResult<T>) {
    match result {
        Err(BigQueryError::SystemError(err)) => {
            assert_eq!(err.public.code, "UNEXPECTED_RESPONSE", "{what}: {err}");
        }
        other => panic!("{what}: expected an unexpected response, got {other:?}"),
    }
}

#[test]
fn a_resource_without_its_reference_is_an_unexpected_response() {
    assert_unexpected("dataset", BigQueryDataset::try_from(v2::Dataset::default()));
    assert_unexpected(
        "listed dataset",
        BigQueryDatasetSummary::try_from(v2::ListFormatDataset::default()),
    );
    assert_unexpected("table", BigQueryTable::try_from(v2::Table::default()));
    assert_unexpected(
        "listed table",
        BigQueryTableSummary::try_from(v2::ListFormatTable::default()),
    );
    assert_unexpected("job", BigQueryJob::try_from(v2::Job::default()));
    assert_unexpected(
        "listed job",
        BigQueryJob::try_from(v2::ListFormatJob::default()),
    );
}

#[tokio::test]
async fn primary_key_columns_are_read_in_key_order_and_empty_without_a_key() {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        if call.method() != "GetTable" {
            return unexpected(call).await;
        }
        let request = call.get_table_request().await;
        let keyed = request.table_id == "order_lines";
        call.reply(&v2::Table {
            table_constraints: keyed.then(|| v2::TableConstraints {
                primary_key: Some(v2::PrimaryKey {
                    columns: vec!["order_id".into(), "line".into()],
                }),
                foreign_keys: Vec::new(),
            }),
            ..Default::default()
        });
    })
    .await;
    let order_lines = SHOP.table(BigQueryTableId::from_static("order_lines"));

    let keyed = fake
        .db
        .primary_key_columns(&order_lines)
        .await
        .expect("the call succeeds");
    let unkeyed = fake
        .db
        .primary_key_columns(&SHOP.table(ORDERS))
        .await
        .expect("the call succeeds");

    assert_eq!(keyed, ["order_id", "line"]);
    assert_eq!(unkeyed, Vec::<String>::new());
    assert_eq!(
        fake.calls(),
        ["GetTable shop.order_lines", "GetTable shop.orders"]
    );
}
