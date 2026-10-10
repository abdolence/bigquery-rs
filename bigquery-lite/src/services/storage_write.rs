//! `google.cloud.bigquery.storage.v1.BigQueryWrite`. This release serves no write streams, so
//! every method answers `Unimplemented` by name.

use super::unimplemented;
use crate::proto::storage_v1::big_query_write_server::BigQueryWrite;
use futures::stream::Empty;
use gcloud_sdk::google::cloud::bigquery::storage::v1 as storage;
use tonic::{Request, Response, Status, Streaming};

pub(crate) struct StorageWrite;

#[tonic::async_trait]
impl BigQueryWrite for StorageWrite {
    async fn create_write_stream(
        &self,
        _request: Request<storage::CreateWriteStreamRequest>,
    ) -> Result<Response<storage::WriteStream>, Status> {
        unimplemented("google.cloud.bigquery.storage.v1.BigQueryWrite/CreateWriteStream")
    }

    type AppendRowsStream = Empty<Result<storage::AppendRowsResponse, Status>>;

    async fn append_rows(
        &self,
        _request: Request<Streaming<storage::AppendRowsRequest>>,
    ) -> Result<Response<Self::AppendRowsStream>, Status> {
        unimplemented("google.cloud.bigquery.storage.v1.BigQueryWrite/AppendRows")
    }

    async fn get_write_stream(
        &self,
        _request: Request<storage::GetWriteStreamRequest>,
    ) -> Result<Response<storage::WriteStream>, Status> {
        unimplemented("google.cloud.bigquery.storage.v1.BigQueryWrite/GetWriteStream")
    }

    async fn finalize_write_stream(
        &self,
        _request: Request<storage::FinalizeWriteStreamRequest>,
    ) -> Result<Response<storage::FinalizeWriteStreamResponse>, Status> {
        unimplemented("google.cloud.bigquery.storage.v1.BigQueryWrite/FinalizeWriteStream")
    }

    async fn batch_commit_write_streams(
        &self,
        _request: Request<storage::BatchCommitWriteStreamsRequest>,
    ) -> Result<Response<storage::BatchCommitWriteStreamsResponse>, Status> {
        unimplemented("google.cloud.bigquery.storage.v1.BigQueryWrite/BatchCommitWriteStreams")
    }

    async fn flush_rows(
        &self,
        _request: Request<storage::FlushRowsRequest>,
    ) -> Result<Response<storage::FlushRowsResponse>, Status> {
        unimplemented("google.cloud.bigquery.storage.v1.BigQueryWrite/FlushRows")
    }
}
