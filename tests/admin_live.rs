//! The plain dataset, table and job calls a test account limited to the CI dataset may make:
//! reading the dataset, a table's whole life in it, and a query job. Dataset creation, update
//! and deletion are left to the unit tests, since the account may not touch any dataset's
//! metadata. Every call is metadata except one `COUNT(*)` over an empty table.

use bigquery::errors::BigQueryError;
use bigquery::*;
use futures::StreamExt;
use std::time::{Duration, Instant};

#[path = "support/common.rs"]
mod common;
use common::*;

const ORDERS: &str = "orders";
const CUSTOMERS: &str = "customers";

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

/// The listing of `job`, polled until it appears. `ListJobs` makes no promise to show a job as
/// soon as `GetJob` does: a finished job has been measured missing from its first listing and
/// present 2.6 s later.
async fn listed_job(
    db: &BigQueryDb,
    job_ref: &BigQueryJobRef,
    job: &BigQueryJob,
) -> TestResult<BigQueryJob> {
    const LISTING_DEADLINE: Duration = Duration::from_secs(30);
    const LISTING_RETRY_INTERVAL: Duration = Duration::from_secs(1);
    let since = job.creation_time.ok_or("the job has no creation time")?;
    let started = Instant::now();
    loop {
        let listed = db
            .stream_jobs_with_errors(
                BigQueryListJobsParams::new()
                    .with_min_creation_time(since)
                    .with_states(vec![BigQueryJobState::Done]),
            )
            .await?
            .filter(|listed| {
                let ours = listed
                    .as_ref()
                    .map_or(true, |listed| listed.reference.job_id == job_ref.job_id);
                async move { ours }
            })
            .boxed()
            .next()
            .await
            .transpose()?;
        match listed {
            Some(listed) => return Ok(listed),
            None if started.elapsed() >= LISTING_DEADLINE => {
                return Err("the query job is not listed".into());
            }
            None => tokio::time::sleep(LISTING_RETRY_INTERVAL).await,
        }
    }
}

#[tokio::test]
async fn dataset_table_and_job_calls() -> TestResult {
    with_scratch("dataset_table_and_job_calls", async |scratch: &Scratch| {
        let db = &scratch.db;
        let dataset = db.fluent().schema().dataset(CI_DATASET).get().await?;
        assert_eq!(dataset.reference.project(), Some(scratch.project.as_str()));
        assert_eq!(dataset.location, Some(CI_LOCATION));
        assert!(dataset.creation_time.is_some());

        let listed = db
            .fluent()
            .schema()
            .datasets()
            .stream_all_with_errors()
            .await?
            .filter(|d| {
                let ours = d
                    .as_ref()
                    .map_or(true, |d| d.reference.dataset() == &CI_DATASET);
                async move { ours }
            })
            .boxed()
            .next()
            .await
            .ok_or("the CI dataset is not listed")??;
        assert_eq!(listed.location, Some(CI_LOCATION));

        create_table(db, scratch.table(ORDERS)).await?;
        create_table(db, scratch.table(CUSTOMERS)).await?;
        let table = db
            .fluent()
            .schema()
            .table(scratch.table(ORDERS))
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
        assert_eq!(table.location, Some(CI_LOCATION));

        // Other runs share the dataset, so only this run's tables are compared.
        let ours = [scratch.table_id(CUSTOMERS), scratch.table_id(ORDERS)];
        let mut tables: Vec<_> = db
            .fluent()
            .schema()
            .dataset(CI_DATASET)
            .tables()
            .stream_all_with_errors()
            .await?
            .map(|t| t.map(|t| t.reference.table().clone()))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|table| ours.contains(table))
            .collect();
        tables.sort();
        assert_eq!(tables, ours);

        let outcome = db
            .fluent()
            .query(format!(
                "SELECT COUNT(*) FROM {}",
                scratch.table_sql(ORDERS)
            ))
            .execute()
            .await?;
        let job_ref = outcome.job.ok_or("the query reported no job")?;
        let job = db.get_job(&job_ref).await?;
        assert_eq!(job.state, Some(BigQueryJobState::Done));
        assert_eq!(job.job_type, Some(BigQueryJobType::Query));
        assert_eq!(job.error, None);
        let listed = listed_job(db, &job_ref, &job).await?;
        assert_eq!(listed.statement_type, Some(BigQueryStatementType::Select));

        db.fluent()
            .schema()
            .table(scratch.table(ORDERS))
            .delete()
            .await?;
        assert!(matches!(
            db.fluent()
                .schema()
                .table(scratch.table(ORDERS))
                .get()
                .await,
            Err(BigQueryError::DataNotFoundError(_))
        ));
        Ok(())
    })
    .await
}
