//! A scratch dataset for the live read tests, and the SQL they build their tables with.

use bigquery::errors::BigQueryError;
use bigquery::BigQueryDb;
use futures::FutureExt;
use gcloud_sdk::google::cloud::bigquery::v2 as bq;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::common::{scratch_dataset_id, TestResult};

/// A dataset made for one test, in the US, that expires after two hours if a run dies before
/// [`Scratch::delete`].
pub struct Scratch {
    pub db: BigQueryDb,
    pub project: String,
    pub dataset: bigquery::BigQueryDatasetId,
    bytes_billed: AtomicI64,
}

impl Scratch {
    /// Creates `<prefix>_<timestamp>`.
    pub async fn create(db: BigQueryDb, project: &str, prefix: &str) -> TestResult<Self> {
        let dataset = scratch_dataset_id(prefix)?;
        db.dataset_client()
            .insert_dataset(bq::InsertDatasetRequest {
                project_id: project.to_string(),
                dataset: Some(bq::Dataset {
                    dataset_reference: Some(bq::DatasetReference {
                        dataset_id: dataset.to_string(),
                        project_id: project.to_string(),
                    }),
                    location: "US".into(),
                    default_table_expiration_ms: Some(2 * 3600 * 1000),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .map_err(BigQueryError::from)?;
        Ok(Self {
            db,
            project: project.to_string(),
            dataset,
            bytes_billed: AtomicI64::new(0),
        })
    }

    /// Runs one GoogleSQL statement with this dataset as the default and waits for it.
    #[allow(
        dead_code,
        reason = "sql_live builds its table with a parameterised query"
    )]
    pub async fn sql(&self, sql: &str) -> TestResult {
        let mut response = self
            .db
            .job_client()
            .query(bq::PostQueryRequest {
                project_id: self.project.clone(),
                query_request: Some(bq::QueryRequest {
                    query: sql.to_string(),
                    use_legacy_sql: Some(false),
                    timeout_ms: Some(60_000),
                    default_dataset: Some(bq::DatasetReference {
                        dataset_id: self.dataset.to_string(),
                        project_id: self.project.clone(),
                    }),
                    location: "US".into(),
                    ..Default::default()
                }),
            })
            .await
            .map_err(BigQueryError::from)?
            .into_inner();
        while response.job_complete != Some(true) {
            let job = response
                .job_reference
                .clone()
                .ok_or("an incomplete query has a job reference")?;
            let results = self
                .db
                .job_client()
                .get_query_results(bq::GetQueryResultsRequest {
                    project_id: job.project_id.clone(),
                    job_id: job.job_id.clone(),
                    max_results: Some(0),
                    timeout_ms: Some(60_000),
                    location: "US".into(),
                    ..Default::default()
                })
                .await
                .map_err(BigQueryError::from)?
                .into_inner();
            response.job_complete = results.job_complete;
        }
        let billed = response.total_bytes_billed.unwrap_or_default();
        self.bytes_billed.fetch_add(billed, Ordering::SeqCst);
        Ok(())
    }

    pub fn bytes_billed(&self) -> i64 {
        self.bytes_billed.load(Ordering::SeqCst)
    }

    pub async fn delete(&self) -> TestResult {
        self.db
            .dataset_client()
            .delete_dataset(bq::DeleteDatasetRequest {
                project_id: self.project.clone(),
                dataset_id: self.dataset.to_string(),
                delete_contents: true,
            })
            .await
            .map_err(BigQueryError::from)?;
        Ok(())
    }
}

/// Runs `body` against a fresh `bqp4_*` scratch dataset and deletes the dataset whatever
/// `body` did, a panic included, printing the bytes its statements billed as
/// `LIVE bytes billed <name>: <n>`.
#[allow(dead_code, reason = "each test binary uses one of the two")]
pub async fn with_scratch<F>(name: &str, body: F) -> TestResult
where
    F: for<'s> AsyncFnOnce(&'s Scratch) -> TestResult,
{
    with_scratch_prefixed("bqp4", name, body).await
}

/// [`with_scratch`] with a dataset named `<prefix>_<timestamp>`.
#[allow(dead_code, reason = "each test binary uses one of the two")]
pub async fn with_scratch_prefixed<F>(prefix: &str, name: &str, body: F) -> TestResult
where
    F: for<'s> AsyncFnOnce(&'s Scratch) -> TestResult,
{
    let Some(project) = crate::common::test_project() else {
        eprintln!("GCP_PROJECT is not set, skipping the live test {name}");
        return Ok(());
    };
    let db = crate::common::setup(&project).await?;
    let scratch = Scratch::create(db, &project, prefix).await?;
    // A failed assertion panics, and the dataset has to go even then.
    let result = std::panic::AssertUnwindSafe(body(&scratch))
        .catch_unwind()
        .await;
    let cleanup = scratch.delete().await;
    eprintln!("LIVE bytes billed {name}: {}", scratch.bytes_billed());
    match result {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
}
