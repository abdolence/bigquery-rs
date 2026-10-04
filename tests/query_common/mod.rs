//! A scratch dataset and a job label for the live query tests, and the bytes their jobs billed.

use bigquery::errors::BigQueryError;
use bigquery::BigQueryDb;
use futures::FutureExt;
use gcloud_sdk::google::cloud::bigquery::v2 as bq;

use crate::common::{scratch_dataset_id, TestResult};

/// The label key every live query carries, so that its jobs can be found and their bytes
/// billed summed.
pub const RUN_LABEL: &str = "bqp5_run";

/// One live test's client, its run label value, and the dataset made for it, if any.
pub struct Live {
    pub db: BigQueryDb,
    pub project: String,
    /// The run label's value, also the scratch dataset's name when there is one.
    pub run: bigquery::BigQueryDatasetId,
    started_ms: u64,
}

impl Live {
    /// The bytes billed by every job of this run, summed from `ListJobs`, which bills nothing.
    pub async fn bytes_billed(&self) -> TestResult<i64> {
        let mut billed = 0;
        let mut page_token = String::new();
        loop {
            let page = self
                .db
                .job_client()
                .list_jobs(bq::ListJobsRequest {
                    project_id: self.project.clone(),
                    min_creation_time: self.started_ms,
                    projection: bq::list_jobs_request::Projection::Full.into(),
                    page_token: page_token.clone(),
                    ..Default::default()
                })
                .await
                .map_err(BigQueryError::from)?
                .into_inner();
            for job in page.jobs {
                let labelled = job
                    .configuration
                    .as_ref()
                    .and_then(|c| c.labels.get(RUN_LABEL))
                    .is_some_and(|run| self.run == run.as_str());
                if labelled {
                    billed += job
                        .statistics
                        .and_then(|s| s.query)
                        .and_then(|q| q.total_bytes_billed)
                        .unwrap_or(0);
                }
            }
            if page.next_page_token.is_empty() {
                return Ok(billed);
            }
            page_token = page.next_page_token;
        }
    }
}

async fn create_dataset(db: &BigQueryDb, project: &str, dataset: &str) -> TestResult {
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
    Ok(())
}

async fn delete_dataset(db: &BigQueryDb, project: &str, dataset: &str) -> TestResult {
    db.dataset_client()
        .delete_dataset(bq::DeleteDatasetRequest {
            project_id: project.to_string(),
            dataset_id: dataset.to_string(),
            delete_contents: true,
        })
        .await
        .map_err(BigQueryError::from)?;
    Ok(())
}

/// Runs `body` against BigQuery when `GCP_PROJECT` is set, with a scratch dataset named after
/// the run if `with_dataset`, deleted whatever `body` did, a panic included. Prints the bytes
/// the run's jobs billed as `LIVE bytes billed <name>: <n>`.
pub async fn live<F>(name: &str, with_dataset: bool, body: F) -> TestResult
where
    F: for<'l> AsyncFnOnce(&'l Live) -> TestResult,
{
    let Some(project) = crate::common::test_project() else {
        eprintln!("GCP_PROJECT is not set, skipping the live test {name}");
        return Ok(());
    };
    let db = crate::common::setup(&project).await?;
    let started_ms = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let live = Live {
        db,
        project,
        run: scratch_dataset_id("bqp5")?,
        started_ms,
    };
    if with_dataset {
        create_dataset(&live.db, &live.project, live.run.as_str()).await?;
    }
    let result = std::panic::AssertUnwindSafe(body(&live))
        .catch_unwind()
        .await;
    let cleanup = if with_dataset {
        delete_dataset(&live.db, &live.project, live.run.as_str()).await
    } else {
        Ok(())
    };
    match live.bytes_billed().await {
        Ok(billed) => eprintln!("LIVE bytes billed {name}: {billed}"),
        Err(err) => eprintln!("LIVE bytes billed {name}: not reported ({err})"),
    }
    match result {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
}
