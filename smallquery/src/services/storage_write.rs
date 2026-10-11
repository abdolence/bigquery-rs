//! `google.cloud.bigquery.storage.v1.BigQueryWrite`. This release serves the `_default` stream:
//! `GetWriteStream` and `AppendRows` on it. The streams `CreateWriteStream` makes, with their
//! finalize, flush and batch commit, answer `Unimplemented` by name.

use super::{project_of, respond, unimplemented, Services};
use crate::proto::storage_v1::big_query_write_server::BigQueryWrite;
use gcloud_sdk::google::cloud::bigquery::storage::v1 as storage;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, Instrument};

pub(crate) struct StorageWrite(pub(super) Services);

const GET_WRITE_STREAM: &str = "google.cloud.bigquery.storage.v1.BigQueryWrite/GetWriteStream";
const APPEND_ROWS: &str = "google.cloud.bigquery.storage.v1.BigQueryWrite/AppendRows";

#[tonic::async_trait]
impl BigQueryWrite for StorageWrite {
    async fn create_write_stream(
        &self,
        _request: Request<storage::CreateWriteStreamRequest>,
    ) -> Result<Response<storage::WriteStream>, Status> {
        unimplemented("google.cloud.bigquery.storage.v1.BigQueryWrite/CreateWriteStream")
    }

    type AppendRowsStream = ReceiverStream<Result<storage::AppendRowsResponse, Status>>;

    /// Handles the connection's requests one at a time, in order, so that its responses come in
    /// request order, as the client relies on, and a slow commit slows only this connection.
    async fn append_rows(
        &self,
        request: Request<Streaming<storage::AppendRowsRequest>>,
    ) -> Result<Response<Self::AppendRowsStream>, Status> {
        let mut requests = request.into_inner();
        let (responses, answered) = mpsc::channel(4);
        let mut connection = self.0.state.writes.connection();
        let services = self.0.clone();
        tokio::spawn(async move {
            loop {
                let request = match requests.message().await {
                    Ok(Some(request)) => request,
                    Ok(None) => return,
                    Err(status) => {
                        debug!(%status, "An AppendRows connection ended.");
                        return;
                    }
                };
                let span = services.span(APPEND_ROWS, project_of(&request.write_stream));
                let response = connection.append(request).instrument(span).await;
                if responses.send(Ok(response)).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(answered)))
    }

    async fn get_write_stream(
        &self,
        request: Request<storage::GetWriteStreamRequest>,
    ) -> Result<Response<storage::WriteStream>, Status> {
        let request = request.into_inner();
        let span = self.0.span(GET_WRITE_STREAM, project_of(&request.name));
        respond(
            GET_WRITE_STREAM,
            self.0
                .state
                .writes
                .stream(&request.name)
                .instrument(span)
                .await,
        )
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
