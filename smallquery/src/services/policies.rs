//! `google.cloud.bigquery.v2.RowAccessPolicyService`. Row access policies do not exist in the
//! engine, so a table never has one; the schema sync lists them before a recreate.

use super::{respond, unimplemented, Services};
use crate::error::EngineError;
use crate::ids::TableRef;
use crate::proto::bigquery_v2::row_access_policy_service_server::RowAccessPolicyService;
use gcloud_sdk::google::cloud::bigquery::v2;
use tonic::{Request, Response, Status};
use tracing::Instrument;

pub(crate) struct RowAccessPolicies(pub(super) Services);

const LIST: &str = "google.cloud.bigquery.v2.RowAccessPolicyService/ListRowAccessPolicies";
const GET: &str = "google.cloud.bigquery.v2.RowAccessPolicyService/GetRowAccessPolicy";
const CREATE: &str = "google.cloud.bigquery.v2.RowAccessPolicyService/CreateRowAccessPolicy";
const UPDATE: &str = "google.cloud.bigquery.v2.RowAccessPolicyService/UpdateRowAccessPolicy";
const DELETE: &str = "google.cloud.bigquery.v2.RowAccessPolicyService/DeleteRowAccessPolicy";
const BATCH_DELETE: &str =
    "google.cloud.bigquery.v2.RowAccessPolicyService/BatchDeleteRowAccessPolicies";

#[tonic::async_trait]
impl RowAccessPolicyService for RowAccessPolicies {
    async fn list_row_access_policies(
        &self,
        request: Request<v2::ListRowAccessPoliciesRequest>,
    ) -> Result<Response<v2::ListRowAccessPoliciesResponse>, Status> {
        let request = request.into_inner();
        let span = self.0.span(LIST, &request.project_id);
        let result = async {
            TableRef::parse(&request.project_id, &request.dataset_id, &request.table_id)?;
            Ok::<_, EngineError>(v2::ListRowAccessPoliciesResponse::default())
        };
        respond(LIST, result.instrument(span).await)
    }

    async fn get_row_access_policy(
        &self,
        _request: Request<v2::GetRowAccessPolicyRequest>,
    ) -> Result<Response<v2::RowAccessPolicy>, Status> {
        unimplemented(GET)
    }

    async fn create_row_access_policy(
        &self,
        _request: Request<v2::CreateRowAccessPolicyRequest>,
    ) -> Result<Response<v2::RowAccessPolicy>, Status> {
        unimplemented(CREATE)
    }

    async fn update_row_access_policy(
        &self,
        _request: Request<v2::UpdateRowAccessPolicyRequest>,
    ) -> Result<Response<v2::RowAccessPolicy>, Status> {
        unimplemented(UPDATE)
    }

    async fn delete_row_access_policy(
        &self,
        _request: Request<v2::DeleteRowAccessPolicyRequest>,
    ) -> Result<Response<()>, Status> {
        unimplemented(DELETE)
    }

    async fn batch_delete_row_access_policies(
        &self,
        _request: Request<v2::BatchDeleteRowAccessPoliciesRequest>,
    ) -> Result<Response<()>, Status> {
        unimplemented(BATCH_DELETE)
    }
}
