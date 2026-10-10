//! `google.cloud.bigquery.v2.JobService`. This release runs no queries or jobs, so every
//! method answers `Unimplemented` by name.

use super::unimplemented;
use crate::proto::bigquery_v2::job_service_server::JobService;
use gcloud_sdk::google::cloud::bigquery::v2;
use tonic::{Request, Response, Status};

pub(crate) struct Jobs;

#[tonic::async_trait]
impl JobService for Jobs {
    async fn cancel_job(
        &self,
        _request: Request<v2::CancelJobRequest>,
    ) -> Result<Response<v2::JobCancelResponse>, Status> {
        unimplemented("google.cloud.bigquery.v2.JobService/CancelJob")
    }

    async fn get_job(
        &self,
        _request: Request<v2::GetJobRequest>,
    ) -> Result<Response<v2::Job>, Status> {
        unimplemented("google.cloud.bigquery.v2.JobService/GetJob")
    }

    async fn insert_job(
        &self,
        _request: Request<v2::InsertJobRequest>,
    ) -> Result<Response<v2::Job>, Status> {
        unimplemented("google.cloud.bigquery.v2.JobService/InsertJob")
    }

    async fn delete_job(
        &self,
        _request: Request<v2::DeleteJobRequest>,
    ) -> Result<Response<()>, Status> {
        unimplemented("google.cloud.bigquery.v2.JobService/DeleteJob")
    }

    async fn list_jobs(
        &self,
        _request: Request<v2::ListJobsRequest>,
    ) -> Result<Response<v2::JobList>, Status> {
        unimplemented("google.cloud.bigquery.v2.JobService/ListJobs")
    }

    async fn get_query_results(
        &self,
        _request: Request<v2::GetQueryResultsRequest>,
    ) -> Result<Response<v2::GetQueryResultsResponse>, Status> {
        unimplemented("google.cloud.bigquery.v2.JobService/GetQueryResults")
    }

    async fn query(
        &self,
        _request: Request<v2::PostQueryRequest>,
    ) -> Result<Response<v2::QueryResponse>, Status> {
        unimplemented("google.cloud.bigquery.v2.JobService/Query")
    }
}
