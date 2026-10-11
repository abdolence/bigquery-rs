//! `google.cloud.bigquery.storage.v1.BigQueryRead`. Sessions serve Arrow; `SplitReadStream` is
//! never served.

use super::{respond, unimplemented, Services};
use crate::error::EngineError;
use crate::proto::storage_v1::big_query_read_server::BigQueryRead;
use futures::stream::{BoxStream, StreamExt};
use gcloud_sdk::google::cloud::bigquery::storage::v1 as storage;
use tonic::{Request, Response, Status};
use tracing::Instrument;

pub(crate) struct StorageRead(pub(super) Services);

const CREATE_READ_SESSION: &str = "google.cloud.bigquery.storage.v1.BigQueryRead/CreateReadSession";
const READ_ROWS: &str = "google.cloud.bigquery.storage.v1.BigQueryRead/ReadRows";

/// The project a Storage API `parent` names, `projects/{project}`, for the span.
fn project_of(parent: &str) -> &str {
    parent.strip_prefix("projects/").unwrap_or(parent)
}

#[tonic::async_trait]
impl BigQueryRead for StorageRead {
    async fn create_read_session(
        &self,
        request: Request<storage::CreateReadSessionRequest>,
    ) -> Result<Response<storage::ReadSession>, Status> {
        let request = request.into_inner();
        let span = self
            .0
            .span(CREATE_READ_SESSION, project_of(&request.parent));
        respond(
            CREATE_READ_SESSION,
            self.0.state.reads.create(request).instrument(span).await,
        )
    }

    type ReadRowsStream = BoxStream<'static, Result<storage::ReadRowsResponse, Status>>;

    async fn read_rows(
        &self,
        request: Request<storage::ReadRowsRequest>,
    ) -> Result<Response<Self::ReadRowsStream>, Status> {
        let request = request.into_inner();
        let _span = self
            .0
            .span(READ_ROWS, project_of(&request.read_stream))
            .entered();
        let rows = self.0.state.reads.read(request).map(|rows| {
            rows.map(|response| response.map_err(|err: EngineError| Status::from(err)))
                .boxed()
        });
        respond(READ_ROWS, rows)
    }

    async fn split_read_stream(
        &self,
        _request: Request<storage::SplitReadStreamRequest>,
    ) -> Result<Response<storage::SplitReadStreamResponse>, Status> {
        unimplemented("google.cloud.bigquery.storage.v1.BigQueryRead/SplitReadStream")
    }
}
