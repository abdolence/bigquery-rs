//! The job calls behind a query: waiting for it, reading its state, and cancelling it.

use crate::errors::{
    BigQueryError, BigQueryErrorPublicGenericDetails, BigQueryJobError, BigQueryJobErrorEntry,
};
use crate::{BigQueryDb, BigQueryJobRef, BigQueryResult};
use gcloud_sdk::google::cloud::bigquery::v2::{
    CancelJobRequest, ErrorProto, GetJobRequest, GetQueryResultsRequest, GetQueryResultsResponse,
    Job,
};
use gcloud_sdk::tonic::metadata::MetadataMap;
use tracing::Span;

impl BigQueryDb {
    /// Asks BigQuery to cancel `job` and returns once the request is accepted, which is before
    /// the job stops: a job that already finished stays finished.
    ///
    /// Dropping a query's stream or future does not cancel its job, so a long query that is no
    /// longer wanted is cancelled with this, by the
    /// [`job`](crate::BigQueryQueryOutcome::job) its outcome named.
    pub async fn cancel_job(&self, job: &BigQueryJobRef) -> BigQueryResult<()> {
        let span = tracing::debug_span!(
            "BigQuery Cancel Job",
            "/bigquery/job_id" = job.job_id.as_str(),
        );
        let request = CancelJobRequest {
            project_id: job.project_id.clone(),
            job_id: job.job_id.to_string(),
            location: job.location_field(),
        };
        self.retry(&span, "cancel a job", &request, &MetadataMap::new(), |r| {
            let mut client = self.job_client();
            async move { client.cancel_job(r).await }
        })
        .await?;
        Ok(())
    }
}

/// Polls `GetQueryResults` with no rows until `job` completes. Each call waits on the server up
/// to `timeout_ms` before answering that the job is still running.
pub(crate) async fn wait_for_job(
    db: &BigQueryDb,
    job: &BigQueryJobRef,
    timeout_ms: u32,
    span: &Span,
) -> BigQueryResult<GetQueryResultsResponse> {
    let request = GetQueryResultsRequest {
        project_id: job.project_id.clone(),
        job_id: job.job_id.to_string(),
        max_results: Some(0),
        timeout_ms: Some(timeout_ms),
        location: job.location_field(),
        ..Default::default()
    };
    loop {
        let response = db
            .retry(
                span,
                "wait for the query job",
                &request,
                &MetadataMap::new(),
                |r| {
                    let mut client = db.job_client();
                    async move { client.get_query_results(r).await }
                },
            )
            .await?;
        if response.job_complete == Some(true) {
            return Ok(response);
        }
    }
}

/// Reads `job`, failing with [`BigQueryError::JobError`] if it finished with an
/// `error_result`.
pub(crate) async fn get_job(
    db: &BigQueryDb,
    job: &BigQueryJobRef,
    span: &Span,
) -> BigQueryResult<Job> {
    let request = GetJobRequest {
        project_id: job.project_id.clone(),
        job_id: job.job_id.to_string(),
        location: job.location_field(),
    };
    let details = db
        .retry(
            span,
            "get the query job",
            &request,
            &MetadataMap::new(),
            |r| {
                let mut client = db.job_client();
                async move { client.get_job(r).await }
            },
        )
        .await?;
    let status = details.status.clone().unwrap_or_default();
    match status.error_result {
        Some(error_result) => Err(BigQueryError::JobError(Box::new(
            BigQueryJobError::new(
                BigQueryErrorPublicGenericDetails::new(error_result.reason.clone()),
                job_error_details(&error_result),
                status.errors.into_iter().map(Into::into).collect(),
            )
            .with_job(job.clone()),
        ))),
        None => Ok(details),
    }
}

fn job_error_details(error: &ErrorProto) -> String {
    if error.location.is_empty() {
        error.message.clone()
    } else {
        format!("{} at {}", error.message, error.location)
    }
}

impl From<ErrorProto> for BigQueryJobErrorEntry {
    fn from(error: ErrorProto) -> Self {
        BigQueryJobErrorEntry::new(error.reason, error.location, error.message)
    }
}
