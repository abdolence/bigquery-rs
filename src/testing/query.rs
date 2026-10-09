//! The query RPCs: `Query`, `InsertJob`, `GetQueryResults`, `GetJob` and `CancelJob`.
//!
//! A `Query` call is answered by the first query rule that matches its SQL and parameters. Its
//! rows go inline when the call's `max_results` allows them all, as the client reads a
//! complete inline page without another call.

use crate::db::fake::wire::{IpcCompression, IpcMessages};
use crate::db::fake::FakeCall;
use crate::testing::rules::{QueryAnswer, ShownParameters};
use crate::testing::server::FakeShared;
use crate::testing::state::FakeJob;
use crate::{BigQueryStatementType, BigQueryTableSchema};
use arrow_array::RecordBatch;
use gcloud_sdk::google::cloud::bigquery::v2::query_request::{
    JobCreationMode, ResultsFormatSerializationOptions,
};
use gcloud_sdk::google::cloud::bigquery::v2::query_response::{Results, ResultsSchema};
use gcloud_sdk::google::cloud::bigquery::v2::{
    ArrowRecordBatch, ArrowSchema, JobReference, PostQueryRequest, QueryRequest, QueryResponse,
    TableSchema,
};

/// The location a job reports when the call names none, BigQuery's default.
const DEFAULT_LOCATION: &str = "US";

impl FakeShared {
    pub(super) async fn serve_query(&self, call: FakeCall) {
        match call.method() {
            "Query" => self.post_query(call).await,
            // TODO: answer InsertJob, GetQueryResults, GetJob and CancelJob from the job
            // records, for destination tables, results beyond max_results and failed jobs.
            method => {
                let described = method.to_string();
                self.unmatched(call, &described, &[]);
            }
        }
    }

    /// Answers `Query` from the first query rule that matches it.
    async fn post_query(&self, call: FakeCall) {
        let Some((call, request)) = self.first_request::<PostQueryRequest>(call).await else {
            return;
        };
        let Some(query) = request.query_request else {
            self.unmatched(call, "Query without a query request", &[]);
            return;
        };
        let described = || {
            format!(
                "Query {:?} with {}",
                query.query,
                ShownParameters(&query.query_parameters)
            )
        };
        let answered = self
            .rules()
            .answer_query(&query.query, &query.query_parameters);
        let reply = match answered {
            Ok(Some(reply)) => reply,
            Ok(None) => {
                let rules = self.rules().describe_queries();
                self.unmatched(call, &described(), &rules);
                return;
            }
            Err(failure) => {
                self.internal(call, &failure);
                return;
            }
        };
        match &reply.answer {
            QueryAnswer::Fault(fault) => fault.clone().answer(call).await,
            QueryAnswer::Rows { schema, rows } if !query.dry_run && fits_inline(&query, rows) => {
                let inline = InlineRows {
                    schema,
                    rows,
                    bytes_processed: reply.bytes_processed,
                };
                match inline.response(self, &request.project_id, &query) {
                    Ok(response) => call.reply(&response),
                    Err(failure) => self.internal(call, &failure),
                }
            }
            // TODO: answer dry runs, DML, statements without rows, failed jobs, and rows beyond
            // max_results through a job and its result table.
            _ => {
                let rules = self.rules().describe_queries();
                self.unmatched(call, &format!("{} beyond inline rows", described()), &rules);
            }
        }
    }
}

/// Whether the call's `max_results` allows all of `rows` inline.
fn fits_inline(query: &QueryRequest, rows: &RecordBatch) -> bool {
    query
        .max_results
        .is_none_or(|max| usize::try_from(max).is_ok_and(|max| max >= rows.num_rows()))
}

/// A rule's rows, answered inline in the `Query` response.
struct InlineRows<'r> {
    schema: &'r BigQueryTableSchema,
    rows: &'r RecordBatch,
    bytes_processed: Option<i64>,
}

impl InlineRows<'_> {
    /// The complete `Query` response for `query`, sent in `project`. It carries a job when the
    /// call requires one, as every retry does, and a query ID otherwise.
    ///
    /// # Errors
    /// An internal failure of the fake if the rows do not encode as Arrow IPC.
    fn response(
        &self,
        shared: &FakeShared,
        project: &str,
        query: &QueryRequest,
    ) -> Result<QueryResponse, String> {
        let batches = if self.rows.num_rows() == 0 {
            &[][..]
        } else {
            std::slice::from_ref(self.rows)
        };
        let messages = IpcMessages::encode(
            self.rows.schema_ref(),
            batches,
            requested_compression(query),
        )
        .map_err(|err| format!("the rows do not encode as Arrow IPC: {err}"))?;
        let total_rows = u64::try_from(self.rows.num_rows()).ok();
        let location = if query.location.is_empty() {
            DEFAULT_LOCATION.to_string()
        } else {
            query.location.clone()
        };
        let mut response = QueryResponse {
            schema: Some(TableSchema::from(self.schema)),
            location: location.clone(),
            total_rows,
            total_bytes_processed: self.bytes_processed,
            job_complete: Some(true),
            statement_type: BigQueryStatementType::Select.as_str().to_string(),
            results_schema: Some(ResultsSchema::ArrowSchema(ArrowSchema {
                serialized_schema: messages.schema,
            })),
            // A result without rows comes as its schema and an empty record batch message.
            results: Some(Results::ArrowRecordBatch(ArrowRecordBatch {
                serialized_record_batch: messages.batches.into_iter().next().unwrap_or_default(),
            })),
            ..Default::default()
        };
        let mut state = shared.state();
        if query.job_creation_mode() == JobCreationMode::JobCreationRequired {
            let job_id = state.add_job(FakeJob {
                location: location.clone(),
                statement_type: BigQueryStatementType::Select,
                complete: true,
                cancelled: false,
                destination: None,
                total_rows,
                bytes_processed: self.bytes_processed,
                dml: None,
                failure: None,
            });
            response.job_reference = Some(JobReference {
                project_id: project.to_string(),
                job_id: job_id.to_string(),
                location: Some(location),
            });
        } else {
            response.query_id = format!("fake-query-{}", state.next_id());
        }
        Ok(response)
    }
}

/// The buffer compression the call asked its Arrow results in.
fn requested_compression(query: &QueryRequest) -> IpcCompression {
    match &query.results_format_serialization_options {
        Some(ResultsFormatSerializationOptions::ArrowSerializationOptions(options)) => {
            options.buffer_compression().into()
        }
        None => IpcCompression::default(),
    }
}
