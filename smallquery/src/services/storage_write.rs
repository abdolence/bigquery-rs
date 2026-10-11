//! `google.cloud.bigquery.storage.v1.BigQueryWrite`: the `_default` stream of every table, and
//! the committed, pending and buffered streams `CreateWriteStream` makes, with their finalize,
//! flush and batch commit.

use super::{project_of, respond, Services};
use crate::proto::storage_v1::big_query_write_server::BigQueryWrite;
use gcloud_sdk::google::cloud::bigquery::storage::v1 as storage;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, Instrument};

pub(crate) struct StorageWrite(pub(super) Services);

const CREATE_WRITE_STREAM: &str =
    "google.cloud.bigquery.storage.v1.BigQueryWrite/CreateWriteStream";
const GET_WRITE_STREAM: &str = "google.cloud.bigquery.storage.v1.BigQueryWrite/GetWriteStream";
const APPEND_ROWS: &str = "google.cloud.bigquery.storage.v1.BigQueryWrite/AppendRows";
const FINALIZE_WRITE_STREAM: &str =
    "google.cloud.bigquery.storage.v1.BigQueryWrite/FinalizeWriteStream";
const BATCH_COMMIT_WRITE_STREAMS: &str =
    "google.cloud.bigquery.storage.v1.BigQueryWrite/BatchCommitWriteStreams";
const FLUSH_ROWS: &str = "google.cloud.bigquery.storage.v1.BigQueryWrite/FlushRows";

#[tonic::async_trait]
impl BigQueryWrite for StorageWrite {
    async fn create_write_stream(
        &self,
        request: Request<storage::CreateWriteStreamRequest>,
    ) -> Result<Response<storage::WriteStream>, Status> {
        let request = request.into_inner();
        let span = self
            .0
            .span(CREATE_WRITE_STREAM, project_of(&request.parent));
        respond(
            CREATE_WRITE_STREAM,
            self.0
                .state
                .writes
                .create_stream(&request)
                .instrument(span)
                .await,
        )
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
            // Only a connection's first request has to name its stream.
            let mut project = String::new();
            loop {
                let request = match requests.message().await {
                    Ok(Some(request)) => request,
                    Ok(None) => return,
                    Err(status) => {
                        debug!(%status, "An AppendRows connection ended.");
                        return;
                    }
                };
                if !request.write_stream.is_empty() {
                    project = project_of(&request.write_stream).to_string();
                }
                let span = services.span(APPEND_ROWS, &project);
                let response = connection.append(request).instrument(span).await;
                #[cfg(test)]
                if services.state.writes.take_lost_response() {
                    return;
                }
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
        request: Request<storage::FinalizeWriteStreamRequest>,
    ) -> Result<Response<storage::FinalizeWriteStreamResponse>, Status> {
        let request = request.into_inner();
        let span = self
            .0
            .span(FINALIZE_WRITE_STREAM, project_of(&request.name));
        respond(
            FINALIZE_WRITE_STREAM,
            self.0
                .state
                .writes
                .finalize(&request.name)
                .instrument(span)
                .await,
        )
    }

    async fn batch_commit_write_streams(
        &self,
        request: Request<storage::BatchCommitWriteStreamsRequest>,
    ) -> Result<Response<storage::BatchCommitWriteStreamsResponse>, Status> {
        let request = request.into_inner();
        let span = self
            .0
            .span(BATCH_COMMIT_WRITE_STREAMS, project_of(&request.parent));
        respond(
            BATCH_COMMIT_WRITE_STREAMS,
            self.0
                .state
                .writes
                .batch_commit(&request)
                .instrument(span)
                .await,
        )
    }

    async fn flush_rows(
        &self,
        request: Request<storage::FlushRowsRequest>,
    ) -> Result<Response<storage::FlushRowsResponse>, Status> {
        let request = request.into_inner();
        let span = self.0.span(FLUSH_ROWS, project_of(&request.write_stream));
        respond(
            FLUSH_ROWS,
            self.0.state.writes.flush(&request).instrument(span).await,
        )
    }
}
