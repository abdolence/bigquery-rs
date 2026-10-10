//! `google.cloud.bigquery.storage.v1.BigQueryRead`. This release serves no read sessions, so
//! every method answers `Unimplemented` by name; `SplitReadStream` is never served.

use super::unimplemented;
use crate::proto::storage_v1::big_query_read_server::BigQueryRead;
use futures::stream::Empty;
use gcloud_sdk::google::cloud::bigquery::storage::v1 as storage;
use tonic::{Request, Response, Status};

pub(crate) struct StorageRead;

#[tonic::async_trait]
impl BigQueryRead for StorageRead {
    async fn create_read_session(
        &self,
        _request: Request<storage::CreateReadSessionRequest>,
    ) -> Result<Response<storage::ReadSession>, Status> {
        unimplemented("google.cloud.bigquery.storage.v1.BigQueryRead/CreateReadSession")
    }

    type ReadRowsStream = Empty<Result<storage::ReadRowsResponse, Status>>;

    async fn read_rows(
        &self,
        _request: Request<storage::ReadRowsRequest>,
    ) -> Result<Response<Self::ReadRowsStream>, Status> {
        unimplemented("google.cloud.bigquery.storage.v1.BigQueryRead/ReadRows")
    }

    async fn split_read_stream(
        &self,
        _request: Request<storage::SplitReadStreamRequest>,
    ) -> Result<Response<storage::SplitReadStreamResponse>, Status> {
        unimplemented("google.cloud.bigquery.storage.v1.BigQueryRead/SplitReadStream")
    }
}
