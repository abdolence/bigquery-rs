//! Fake answers for the query RPCs: `Query`, `InsertJob`, `GetQueryResults`, `GetJob` and
//! `CancelJob`.

use super::FakeCall;
use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use gcloud_sdk::google::cloud::bigquery::v2::{
    query_response, ArrowRecordBatch, ArrowSchema, CancelJobRequest, GetJobRequest,
    GetQueryResultsRequest, InsertJobRequest, Job, JobReference, PostQueryRequest, QueryResponse,
};

/// The job every fake query runs as.
pub(crate) fn job_reference() -> JobReference {
    JobReference {
        project_id: "fake-project".into(),
        job_id: "job1".into(),
        location: Some("US".into()),
    }
}

impl FakeCall {
    /// Reads a `Query` request and logs it as `Query <sql>`.
    pub(crate) async fn query_request(&mut self) -> PostQueryRequest {
        let request: PostQueryRequest = self.next_request().await.expect("a Query request");
        let sql = request
            .query_request
            .as_ref()
            .map(|q| q.query.clone())
            .unwrap_or_default();
        self.log(format!("Query {sql}"));
        request
    }

    /// Reads an `InsertJob` request and logs it as
    /// `InsertJob <sql> into <project>.<dataset>.<table> WRITE_EMPTY CREATE_IF_NEEDED`.
    pub(crate) async fn insert_job_request(&mut self) -> InsertJobRequest {
        let request: InsertJobRequest = self.next_request().await.expect("an InsertJob request");
        let query = request
            .job
            .as_ref()
            .and_then(|job| job.configuration.as_ref())
            .and_then(|configuration| configuration.query.clone())
            .unwrap_or_default();
        let destination = query
            .destination_table
            .map(|table| {
                format!(
                    "{}.{}.{}",
                    table.project_id, table.dataset_id, table.table_id
                )
            })
            .unwrap_or_default();
        self.log(format!(
            "InsertJob {} into {destination} {} {}",
            query.query, query.write_disposition, query.create_disposition
        ));
        request
    }

    /// Reads a `GetQueryResults` request and logs it as
    /// `GetQueryResults job1 at US max_results=Some(0)`.
    pub(crate) async fn query_results_request(&mut self) -> GetQueryResultsRequest {
        let request: GetQueryResultsRequest = self
            .next_request()
            .await
            .expect("a GetQueryResults request");
        self.log(format!(
            "GetQueryResults {} at {} max_results={:?}",
            request.job_id, request.location, request.max_results
        ));
        request
    }

    /// Reads a `GetJob` request and logs it as `GetJob job1 at US`.
    pub(crate) async fn get_job_request(&mut self) -> GetJobRequest {
        let request: GetJobRequest = self.next_request().await.expect("a GetJob request");
        self.log(format!("GetJob {} at {}", request.job_id, request.location));
        request
    }

    /// Reads a `CancelJob` request and logs it as `CancelJob job1 at EU`.
    pub(crate) async fn cancel_job_request(&mut self) -> CancelJobRequest {
        let request: CancelJobRequest = self.next_request().await.expect("a CancelJob request");
        self.log(format!(
            "CancelJob {} at {}",
            request.job_id, request.location
        ));
        request
    }
}

/// The IPC schema message of `batch` and its record batch message, uncompressed, as the inline
/// result of a `Query` response carries them.
pub(crate) fn encode_ipc(batch: &RecordBatch) -> (Vec<u8>, Vec<u8>) {
    let mut writer = StreamWriter::try_new(Vec::new(), &batch.schema()).expect("an IPC writer");
    let schema = writer.get_ref().clone();
    writer.write(batch).expect("the batch encodes");
    let message = writer.get_ref()[schema.len()..].to_vec();
    (schema, message)
}

/// A complete `Query` response carrying `batch` inline, of a result of `total_rows` rows.
pub(crate) fn inline_response(batch: &RecordBatch, total_rows: u64) -> QueryResponse {
    let (schema, message) = encode_ipc(batch);
    QueryResponse {
        job_reference: Some(job_reference()),
        job_complete: Some(true),
        total_rows: Some(total_rows),
        statement_type: "SELECT".into(),
        results_schema: Some(query_response::ResultsSchema::ArrowSchema(ArrowSchema {
            serialized_schema: schema,
        })),
        results: Some(query_response::Results::ArrowRecordBatch(
            ArrowRecordBatch {
                serialized_record_batch: message,
            },
        )),
        ..Default::default()
    }
}

/// A `Query` response for a job still running.
pub(crate) fn incomplete_response() -> QueryResponse {
    QueryResponse {
        job_reference: Some(job_reference()),
        job_complete: Some(false),
        ..Default::default()
    }
}

/// The fake job, done, with its result in `ds.table`.
pub(crate) fn done_job(dataset: &str, table: &str) -> Job {
    use gcloud_sdk::google::cloud::bigquery::v2::{
        JobConfiguration, JobConfigurationQuery, JobStatus, TableReference,
    };
    Job {
        job_reference: Some(job_reference()),
        configuration: Some(JobConfiguration {
            query: Some(JobConfigurationQuery {
                destination_table: Some(TableReference {
                    project_id: "fake-project".into(),
                    dataset_id: dataset.into(),
                    table_id: table.into(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
        status: Some(JobStatus {
            state: "DONE".into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}
