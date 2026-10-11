//! `google.cloud.bigquery.v2.JobService`: queries and query jobs.

use super::{respond, Services};
use crate::proto::bigquery_v2::job_service_server::JobService;
use gcloud_sdk::google::cloud::bigquery::v2;
use tonic::{Request, Response, Status};
use tracing::Instrument;

pub(crate) struct Jobs(pub(super) Services);

const CANCEL: &str = "google.cloud.bigquery.v2.JobService/CancelJob";
const GET: &str = "google.cloud.bigquery.v2.JobService/GetJob";
const INSERT: &str = "google.cloud.bigquery.v2.JobService/InsertJob";
const DELETE: &str = "google.cloud.bigquery.v2.JobService/DeleteJob";
const LIST: &str = "google.cloud.bigquery.v2.JobService/ListJobs";
const GET_QUERY_RESULTS: &str = "google.cloud.bigquery.v2.JobService/GetQueryResults";
const QUERY: &str = "google.cloud.bigquery.v2.JobService/Query";

#[tonic::async_trait]
impl JobService for Jobs {
    async fn cancel_job(
        &self,
        request: Request<v2::CancelJobRequest>,
    ) -> Result<Response<v2::JobCancelResponse>, Status> {
        let request = request.into_inner();
        let _span = self.0.span(CANCEL, &request.project_id).entered();
        respond(CANCEL, self.0.state.queries.cancel_job(&request))
    }

    async fn get_job(
        &self,
        request: Request<v2::GetJobRequest>,
    ) -> Result<Response<v2::Job>, Status> {
        let request = request.into_inner();
        let _span = self.0.span(GET, &request.project_id).entered();
        respond(GET, self.0.state.queries.get_job(&request))
    }

    async fn insert_job(
        &self,
        request: Request<v2::InsertJobRequest>,
    ) -> Result<Response<v2::Job>, Status> {
        let request = request.into_inner();
        let span = self.0.span(INSERT, &request.project_id);
        respond(
            INSERT,
            self.0
                .state
                .queries
                .insert_job(request)
                .instrument(span)
                .await,
        )
    }

    async fn delete_job(
        &self,
        request: Request<v2::DeleteJobRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let _span = self.0.span(DELETE, &request.project_id).entered();
        respond(DELETE, self.0.state.queries.delete_job(&request))
    }

    async fn list_jobs(
        &self,
        request: Request<v2::ListJobsRequest>,
    ) -> Result<Response<v2::JobList>, Status> {
        let request = request.into_inner();
        let _span = self.0.span(LIST, &request.project_id).entered();
        respond(LIST, self.0.state.queries.list_jobs(&request))
    }

    async fn get_query_results(
        &self,
        request: Request<v2::GetQueryResultsRequest>,
    ) -> Result<Response<v2::GetQueryResultsResponse>, Status> {
        let request = request.into_inner();
        let span = self.0.span(GET_QUERY_RESULTS, &request.project_id);
        respond(
            GET_QUERY_RESULTS,
            self.0
                .state
                .queries
                .get_query_results(request)
                .instrument(span)
                .await,
        )
    }

    async fn query(
        &self,
        request: Request<v2::PostQueryRequest>,
    ) -> Result<Response<v2::QueryResponse>, Status> {
        let request = request.into_inner();
        let span = self.0.span(QUERY, &request.project_id);
        respond(
            QUERY,
            self.0.state.queries.query(request).instrument(span).await,
        )
    }
}
