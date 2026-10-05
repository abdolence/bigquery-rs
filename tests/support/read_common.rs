//! The SQL the live read tests build their tables with.

use bigquery::errors::BigQueryError;
use gcloud_sdk::google::cloud::bigquery::v2 as bq;

use crate::common::{Scratch, TestResult, CI_LOCATION};

/// Runs one GoogleSQL statement in the CI dataset's location and waits for it.
pub async fn run_sql(s: &Scratch, sql: &str) -> TestResult {
    let mut response =
        s.db.job_client()
            .query(bq::PostQueryRequest {
                project_id: s.project.clone(),
                query_request: Some(bq::QueryRequest {
                    query: sql.to_string(),
                    use_legacy_sql: Some(false),
                    timeout_ms: Some(60_000),
                    location: CI_LOCATION.to_string(),
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
        let results =
            s.db.job_client()
                .get_query_results(bq::GetQueryResultsRequest {
                    project_id: job.project_id.clone(),
                    job_id: job.job_id.clone(),
                    max_results: Some(0),
                    timeout_ms: Some(60_000),
                    location: CI_LOCATION.to_string(),
                    ..Default::default()
                })
                .await
                .map_err(BigQueryError::from)?
                .into_inner();
        response.job_complete = results.job_complete;
    }
    Ok(())
}
