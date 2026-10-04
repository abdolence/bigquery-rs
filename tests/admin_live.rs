//! The plain dataset, table and job calls on one scratch dataset, from its creation to its
//! deletion. Every call is metadata except one `COUNT(*)` over an empty table.

use bigquery::errors::BigQueryError;
use bigquery::*;
use futures::{FutureExt, StreamExt};
use std::panic::AssertUnwindSafe;

#[path = "support/common.rs"]
mod common;
use common::*;

const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
const CUSTOMERS: BigQueryTableId = BigQueryTableId::from_static("customers");

fn columns(c: BigQuerySchemaColumnsBuilder) -> Vec<BigQuerySchemaColumn> {
    c.fields([c.field("id").int64().required(), c.field("name").string()])
}

async fn create_table(db: &BigQueryDb, table: BigQueryTableRef) -> TestResult {
    db.fluent()
        .schema()
        .table(table)
        .columns(columns)
        .sync()
        .await?;
    Ok(())
}

async fn crud(db: &BigQueryDb, project: &str, dataset: &BigQueryDatasetId) -> TestResult {
    let created = db
        .fluent()
        .schema()
        .dataset(dataset.clone())
        .create()
        .location(BigQueryLocation::from_static("EU"))
        .description("scratch")
        .labels([("purpose", "bq_admin_live")])
        .execute()
        .await?;
    assert_eq!(created.reference.project(), Some(project));
    assert_eq!(created.location, Some(BigQueryLocation::from_static("EU")));

    let read = db.fluent().schema().dataset(dataset.clone()).get().await?;
    assert_eq!(read.description.as_deref(), Some("scratch"));
    assert_eq!(read.labels.get("purpose"), Some("bq_admin_live"));
    assert!(read.creation_time.is_some());

    let listed = db
        .fluent()
        .schema()
        .datasets()
        .stream_all_with_errors()
        .await?
        .filter(|d| {
            let ours = d
                .as_ref()
                .map_or(true, |d| d.reference.dataset() == dataset);
            async move { ours }
        })
        .boxed()
        .next()
        .await
        .ok_or("the scratch dataset is not listed")??;
    assert_eq!(listed.location, Some(BigQueryLocation::from_static("EU")));

    let updated = db
        .fluent()
        .schema()
        .dataset(dataset.clone())
        .update()
        .remove_label("purpose")
        .label("stage", "updated")
        .description("updated scratch")
        .execute()
        .await?;
    assert_eq!(
        updated.labels.into_iter().collect::<Vec<_>>(),
        [("stage".to_string(), "updated".to_string())]
    );
    let read = db.fluent().schema().dataset(dataset.clone()).get().await?;
    assert_eq!(read.description.as_deref(), Some("updated scratch"));
    db.fluent()
        .schema()
        .dataset(dataset.clone())
        .update()
        .clear_description()
        .execute()
        .await?;
    let read = db.fluent().schema().dataset(dataset.clone()).get().await?;
    assert_eq!(read.description, None);

    create_table(db, dataset.table(ORDERS)).await?;
    create_table(db, dataset.table(CUSTOMERS)).await?;
    let table = db
        .fluent()
        .schema()
        .table(dataset.table(ORDERS))
        .get()
        .await?;
    assert_eq!(table.table_type, Some(BigQueryTableType::Table));
    let types: Vec<_> = table
        .schema
        .fields
        .iter()
        .map(|f| (f.name.as_str(), f.field_type.clone(), f.mode))
        .collect();
    assert_eq!(
        types,
        [
            ("id", BigQueryFieldType::Int64, BigQueryFieldMode::Required),
            (
                "name",
                BigQueryFieldType::String { max_length: None },
                BigQueryFieldMode::Nullable
            ),
        ]
    );
    assert_eq!(table.num_rows, Some(0));
    assert_eq!(table.location, Some(BigQueryLocation::from_static("EU")));
    let mut tables: Vec<_> = db
        .fluent()
        .schema()
        .dataset(dataset.clone())
        .tables()
        .stream_all_with_errors()
        .await?
        .map(|t| t.map(|t| t.reference.table().clone()))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;
    tables.sort();
    assert_eq!(tables, [CUSTOMERS, ORDERS]);

    let outcome = db
        .fluent()
        .query(format!("SELECT COUNT(*) FROM `{dataset}.{ORDERS}`"))
        .execute()
        .await?;
    let job_ref = outcome.job.ok_or("the query reported no job")?;
    let job = db.get_job(&job_ref).await?;
    assert_eq!(job.state, Some(BigQueryJobState::Done));
    assert_eq!(job.job_type, Some(BigQueryJobType::Query));
    assert_eq!(job.error, None);
    eprintln!(
        "LIVE bytes billed admin_crud: {:?} over 1 job",
        job.total_bytes_billed
    );
    let since = job.creation_time.ok_or("the job has no creation time")?;
    let listed = db
        .stream_jobs_with_errors(
            BigQueryListJobsParams::new()
                .with_min_creation_time(since)
                .with_states(vec![BigQueryJobState::Done]),
        )
        .await?
        .filter(|j| {
            let ours = j
                .as_ref()
                .map_or(true, |j| j.reference.job_id == job_ref.job_id);
            async move { ours }
        })
        .boxed()
        .next()
        .await
        .ok_or("the query job is not listed")??;
    assert_eq!(listed.statement_type, Some(BigQueryStatementType::Select));
    db.delete_job(&job_ref).await?;
    assert!(matches!(
        db.get_job(&job_ref).await,
        Err(BigQueryError::DataNotFoundError(_))
    ));

    db.fluent()
        .schema()
        .table(dataset.table(ORDERS))
        .delete()
        .await?;
    assert!(matches!(
        db.fluent()
            .schema()
            .table(dataset.table(ORDERS))
            .get()
            .await,
        Err(BigQueryError::DataNotFoundError(_))
    ));
    let refused = db.fluent().schema().dataset(dataset.clone()).delete().await;
    assert!(refused.is_err(), "a dataset holding a table was deleted");
    db.fluent()
        .schema()
        .dataset(dataset.clone())
        .dangerously_delete_with_contents()
        .await?;
    assert!(matches!(
        db.fluent().schema().dataset(dataset.clone()).get().await,
        Err(BigQueryError::DataNotFoundError(_))
    ));
    Ok(())
}

#[tokio::test]
async fn dataset_table_and_job_crud() -> TestResult {
    let Some(project) = test_project() else {
        eprintln!("GCP_PROJECT is not set, skipping the live test dataset_table_and_job_crud");
        return Ok(());
    };
    let db = setup(&project).await?;
    let dataset = scratch_dataset_id("bq_admin_live")?;
    let result = AssertUnwindSafe(crud(&db, &project, &dataset))
        .catch_unwind()
        .await;
    let cleanup = db
        .fluent()
        .schema()
        .dataset(dataset.clone())
        .dangerously_delete_with_contents()
        .await;
    match &cleanup {
        Ok(()) | Err(BigQueryError::DataNotFoundError(_)) => {}
        Err(err) => eprintln!("failed to delete the scratch dataset {dataset}: {err}"),
    }
    match result {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    Ok(())
}
