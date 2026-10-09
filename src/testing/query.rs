//! The query RPCs: `Query`, `InsertJob`, `GetQueryResults`, `GetJob` and `CancelJob`.
//!
//! A `Query` or `InsertJob` call is answered by the first query rule that matches its SQL and
//! parameters. A `Query` result goes inline when the call's `max_results` allows all its rows,
//! as the client reads a complete inline page without another call. Otherwise it runs as a job
//! whose result table, in [`QUERY_RESULTS_DATASET`], the client reads through the Storage Read
//! API. `InsertJob` writes the result into the caller's destination table instead.

use crate::db::fake::wire::{IpcCompression, IpcMessages};
use crate::db::fake::FakeCall;
use crate::testing::rules::{BigQueryFakeRpc, QueryAnswer, QueryReply, ShownParameters};
use crate::testing::server::FakeShared;
use crate::testing::state::{
    fit_to_layout, location_or_default, DatasetKey, FakeJob, FakeState, FakeTable, TableKey,
    QUERY_RESULTS_DATASET,
};
use crate::types::kind::FieldKind;
use crate::{
    BigQueryDmlStats, BigQueryFieldMode, BigQueryFieldSchema, BigQueryFieldType, BigQueryJobId,
    BigQueryStatementType, BigQueryTableId, BigQueryTableSchema,
};
use arrow_array::RecordBatch;
use gcloud_sdk::google::cloud::bigquery::v2::query_request::{
    JobCreationMode, ResultsFormatSerializationOptions,
};
use gcloud_sdk::google::cloud::bigquery::v2::query_response::{Results, ResultsSchema};
use gcloud_sdk::google::cloud::bigquery::v2::{
    ArrowRecordBatch, ArrowSchema, CancelJobRequest, DmlStats, ErrorProto, GetJobRequest,
    GetQueryResultsRequest, GetQueryResultsResponse, InsertJobRequest, Job, JobCancelResponse,
    JobConfiguration, JobConfigurationQuery, JobReference, JobStatistics, JobStatistics2,
    JobStatus, PostQueryRequest, QueryParameter, QueryRequest, QueryResponse, TableSchema,
};
use gcloud_sdk::tonic::Status;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// The page token of a result beyond its first page. The client never sends it back: it reads
/// the whole result from the job's table.
const NEXT_PAGE_TOKEN: &str = "fake-page-2";

impl FakeShared {
    pub(super) async fn serve_query(&self, call: FakeCall) {
        match call.method() {
            "Query" => self.post_query(call).await,
            "InsertJob" => self.insert_job(call).await,
            "GetQueryResults" => self.get_query_results(call).await,
            "GetJob" => self.get_job(call).await,
            "CancelJob" => self.cancel_job(call).await,
            method => {
                let described = method.to_string();
                self.unmatched(call, &described, &[]);
            }
        }
    }

    /// The reply of the first query rule that matches `sql` and `parameters`, other than a
    /// fault. A call no rule answers, or whose rule answers with a fault, is answered here and
    /// `None` is returned. `rpc` names the call in the unmatched report.
    async fn matching_reply(
        &self,
        call: FakeCall,
        rpc: &str,
        sql: &str,
        parameters: &[QueryParameter],
    ) -> Option<(FakeCall, Arc<QueryReply>)> {
        let answered = self.rules().answer_query(sql, parameters);
        let reply = match answered {
            Ok(Some(reply)) => reply,
            Ok(None) => {
                let described = format!("{rpc} {sql:?} with {}", ShownParameters(parameters));
                let rules = self.rules().describe_queries();
                self.unmatched(call, &described, &rules);
                return None;
            }
            Err(failure) => {
                self.internal(call, &failure);
                return None;
            }
        };
        if let QueryAnswer::Fault(fault) = &reply.answer {
            fault.clone().answer(call).await;
            return None;
        }
        Some((call, reply))
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
        let Some((call, reply)) = self
            .matching_reply(call, "Query", &query.query, &query.query_parameters)
            .await
        else {
            return;
        };
        match self.query_response(&request.project_id, &query, &reply) {
            Ok(response) => call.reply(&response),
            Err(failure) => self.internal(call, &failure),
        }
    }

    /// The `Query` response of `reply`, sent in `project`. It names a job when the call
    /// requires one, as every retry does, or when a later call needs one: rows beyond
    /// `max_results`, read from the job's table, or a job that has not finished. It carries a
    /// query ID otherwise.
    ///
    /// # Errors
    /// An internal failure of the fake if the rows do not encode as Arrow IPC.
    fn query_response(
        &self,
        project: &str,
        query: &QueryRequest,
        reply: &QueryReply,
    ) -> Result<QueryResponse, String> {
        let location = location_or_default(&query.location);
        let job = FakeJob::ran(location.clone(), reply);
        let mut response = QueryResponse {
            location,
            job_complete: Some(job.complete),
            statement_type: job.statement_type.as_str().to_string(),
            total_bytes_processed: reply.bytes_processed,
            num_dml_affected_rows: job.dml.map(BigQueryDmlStats::affected_rows),
            dml_stats: job.dml.map(DmlStats::from),
            ..Default::default()
        };
        if query.dry_run {
            response.schema = reply.answer.schema().map(TableSchema::from);
            return Ok(response);
        }
        let mut needs_job = !job.complete;
        let mut result_table = None;
        if let QueryAnswer::Rows { schema, rows } = &reply.answer {
            let page = first_page(query, rows);
            response.schema = Some(TableSchema::from(schema));
            response.total_rows = job.total_rows;
            let (results_schema, results) = arrow_results(&page, requested_compression(query))?;
            response.results_schema = Some(results_schema);
            response.results = Some(results);
            if page.num_rows() < rows.num_rows() {
                response.page_token = NEXT_PAGE_TOKEN.to_string();
                needs_job = true;
                result_table = Some((schema.clone(), rows.clone()));
            }
        }
        let mut state = self.state();
        if !needs_job && query.job_creation_mode() != JobCreationMode::JobCreationRequired {
            response.query_id = format!("fake-query-{}", state.next_id());
            return Ok(response);
        }
        let job_id = state.add_job(job);
        if let Some((schema, rows)) = result_table {
            let table = state.add_result_table(project, &job_id, schema, rows)?;
            if let Some(job) = state.jobs.get_mut(&job_id) {
                job.destination = Some(table);
            }
        }
        response.job_reference = Some(job_id.reference(project, &response.location));
        Ok(response)
    }

    /// Answers `InsertJob`, which the client sends for a query into its own destination table,
    /// from the first query rule that matches it. The rule's rows are written into that table
    /// as the job's write disposition says, unless the job is a dry run.
    async fn insert_job(&self, call: FakeCall) {
        let Some((call, request)) = self.first_request::<InsertJobRequest>(call).await else {
            return;
        };
        let job = request.job.unwrap_or_default();
        let configuration = job.configuration.unwrap_or_default();
        let (Some(reference), Some(query)) = (job.job_reference, configuration.query) else {
            self.unmatched(call, "InsertJob without a query job", &[]);
            return;
        };
        let project = if reference.project_id.is_empty() {
            request.project_id
        } else {
            reference.project_id
        };
        let location = location_or_default(reference.location.as_deref().unwrap_or_default());
        let job_id = BigQueryJobId::reported(reference.job_id);
        if self.state().jobs.contains_key(&job_id) {
            let status = job_id.already_exists(&project, &location);
            return call.fail(status.code(), status.message());
        }
        let destination = match query
            .destination_table
            .as_ref()
            .map(|table| TableKey::new(&project, &table.dataset_id, &table.table_id))
            .transpose()
        {
            Ok(destination) => destination,
            Err(err) => {
                let described = format!("InsertJob into an invalid table: {err}");
                self.unmatched(call, &described, &[]);
                return;
            }
        };
        let Some((call, reply)) = self
            .matching_reply(call, "InsertJob", &query.query, &query.query_parameters)
            .await
        else {
            return;
        };
        let mut job = FakeJob::ran(location.clone(), reply.as_ref());
        if configuration.dry_run == Some(true) {
            let mut resource = job.resource(&project, &job_id);
            if let Some(statistics) = resource
                .statistics
                .as_mut()
                .and_then(|statistics| statistics.query.as_mut())
            {
                statistics.schema = reply.answer.schema().map(TableSchema::from);
            }
            return call.reply(&resource);
        }
        let mut state = self.state();
        // A call with the same job ID can have passed the check above while this one waited.
        if state.jobs.contains_key(&job_id) {
            drop(state);
            let status = job_id.already_exists(&project, &location);
            return call.fail(status.code(), status.message());
        }
        if let (Some(table), QueryAnswer::Rows { schema, rows }) = (&destination, &reply.answer) {
            let written =
                state.write_query_result(table, &query.write_disposition, schema, rows.clone());
            if let Err(status) = written {
                drop(state);
                return call.fail(status.code(), status.message());
            }
        }
        job.destination = destination;
        let resource = job.resource(&project, &job_id);
        state.jobs.insert(job_id, job);
        drop(state);
        call.reply(&resource);
    }

    /// Answers `GetQueryResults` for a recorded job, which finishes the job: the client calls it
    /// only to wait, with `max_results` 0, so it carries the result's figures and no rows.
    async fn get_query_results(&self, call: FakeCall) {
        let Some((call, request)) = self.first_request::<GetQueryResultsRequest>(call).await else {
            return;
        };
        let rpc = BigQueryFakeRpc::GetQueryResults;
        let Some(call) = self.unfaulted(call, rpc, None).await else {
            return;
        };
        let job_id = BigQueryJobId::reported(request.job_id);
        let response = self
            .state()
            .jobs
            .get_mut(&job_id)
            .filter(|job| job.is_in(&request.location))
            .map(|job| {
                job.complete = true;
                job.results(&request.project_id, &job_id)
            });
        match response {
            Some(response) => call.reply(&response),
            None => {
                let status = job_id.not_found(&request.project_id, &request.location);
                call.fail(status.code(), status.message());
            }
        }
    }

    /// Answers `GetJob` with a recorded job, or `NotFound`.
    async fn get_job(&self, call: FakeCall) {
        let Some((call, request)) = self.first_request::<GetJobRequest>(call).await else {
            return;
        };
        let Some(call) = self.unfaulted(call, BigQueryFakeRpc::GetJob, None).await else {
            return;
        };
        let job_id = BigQueryJobId::reported(request.job_id);
        let job = self
            .state()
            .jobs
            .get(&job_id)
            .filter(|job| job.is_in(&request.location))
            .map(|job| job.resource(&request.project_id, &job_id));
        match job {
            Some(job) => call.reply(&job),
            None => {
                let status = job_id.not_found(&request.project_id, &request.location);
                call.fail(status.code(), status.message());
            }
        }
    }

    /// Answers `CancelJob` for a recorded job, or `NotFound`. A job that has not finished stops
    /// as BigQuery stops it, and a finished job stays as it finished.
    async fn cancel_job(&self, call: FakeCall) {
        let Some((call, request)) = self.first_request::<CancelJobRequest>(call).await else {
            return;
        };
        let Some(call) = self.unfaulted(call, BigQueryFakeRpc::CancelJob, None).await else {
            return;
        };
        let job_id = BigQueryJobId::reported(request.job_id);
        let job = self
            .state()
            .jobs
            .get_mut(&job_id)
            .filter(|job| job.is_in(&request.location))
            .map(|job| {
                if !job.complete {
                    job.complete = true;
                    job.cancelled = true;
                }
                job.resource(&request.project_id, &job_id)
            });
        match job {
            Some(job) => call.reply(&JobCancelResponse {
                kind: "bigquery#jobCancelResponse".to_string(),
                job: Some(job),
            }),
            None => {
                let status = job_id.not_found(&request.project_id, &request.location);
                call.fail(status.code(), status.message());
            }
        }
    }
}

impl QueryAnswer {
    /// The schema of the result, for an answer with rows.
    fn schema(&self) -> Option<&BigQueryTableSchema> {
        match self {
            QueryAnswer::Rows { schema, .. } => Some(schema),
            _ => None,
        }
    }
}

impl FakeJob {
    /// Whether a call that names `location` reaches the job: one that names none does, and
    /// one that names another location than the job's does not, as BigQuery looks a job up
    /// only where it runs.
    fn is_in(&self, location: &str) -> bool {
        location.is_empty() || location.eq_ignore_ascii_case(&self.location)
    }

    /// The job of a statement that ran as `reply` scripts it, without a destination. A job that
    /// is to fail has not finished yet: it fails once `GetQueryResults` sees it done.
    fn ran(location: String, reply: &QueryReply) -> Self {
        let mut job = FakeJob {
            location,
            statement_type: BigQueryStatementType::Select,
            complete: true,
            cancelled: false,
            destination: None,
            total_rows: None,
            bytes_processed: reply.bytes_processed,
            dml: None,
            failure: None,
        };
        match &reply.answer {
            QueryAnswer::Rows { rows, .. } => job.total_rows = u64::try_from(rows.num_rows()).ok(),
            QueryAnswer::Dml {
                statement_type,
                stats,
            } => {
                job.statement_type = statement_type.clone();
                job.dml = Some(*stats);
            }
            QueryAnswer::Statement(statement_type) => {
                job.statement_type = statement_type.clone();
            }
            QueryAnswer::JobFailure(failure) => {
                job.complete = false;
                job.failure = Some(failure.clone());
            }
            QueryAnswer::Fault(_) => {}
        }
        job
    }

    /// Why the job failed, once it has finished: stopped if it was cancelled before it
    /// finished, else the failure it was scripted with.
    fn error_result(&self) -> Option<ErrorProto> {
        if self.cancelled {
            return Some(ErrorProto {
                reason: "stopped".to_string(),
                message: "Job execution was cancelled: User requested cancellation".to_string(),
                ..Default::default()
            });
        }
        self.failure
            .as_ref()
            .filter(|_| self.complete)
            .map(|failure| ErrorProto {
                reason: failure.reason.clone(),
                message: failure.message.clone(),
                ..Default::default()
            })
    }

    /// The job as `GetJob`, `CancelJob` and `InsertJob` return it, named `job_id` in `project`.
    fn resource(&self, project: &str, job_id: &BigQueryJobId) -> Job {
        let error_result = self.error_result();
        Job {
            kind: "bigquery#job".to_string(),
            job_reference: Some(job_id.reference(project, &self.location)),
            configuration: Some(JobConfiguration {
                job_type: "QUERY".to_string(),
                query: Some(JobConfigurationQuery {
                    destination_table: self.destination.as_ref().map(TableKey::reference),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            statistics: Some(JobStatistics {
                total_bytes_processed: self.bytes_processed,
                query: Some(JobStatistics2 {
                    statement_type: self.statement_type.as_str().to_string(),
                    total_bytes_processed: self.bytes_processed,
                    num_dml_affected_rows: self.dml.map(BigQueryDmlStats::affected_rows),
                    dml_stats: self.dml.map(DmlStats::from),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            status: Some(JobStatus {
                state: if self.complete { "DONE" } else { "RUNNING" }.to_string(),
                errors: error_result.clone().into_iter().collect(),
                error_result,
            }),
            ..Default::default()
        }
    }

    /// The `GetQueryResults` response of the job, without rows.
    fn results(&self, project: &str, job_id: &BigQueryJobId) -> GetQueryResultsResponse {
        GetQueryResultsResponse {
            kind: "bigquery#getQueryResultsResponse".to_string(),
            job_reference: Some(job_id.reference(project, &self.location)),
            job_complete: Some(self.complete),
            total_rows: self.total_rows,
            total_bytes_processed: self.bytes_processed,
            num_dml_affected_rows: self.dml.map(BigQueryDmlStats::affected_rows),
            ..Default::default()
        }
    }
}

impl FakeState {
    /// Creates the hidden table that holds the result of the job `job_id` in `project`.
    ///
    /// # Errors
    /// An internal failure of the fake if the job ID does not make a table ID, or the table
    /// exists.
    fn add_result_table(
        &mut self,
        project: &str,
        job_id: &BigQueryJobId,
        schema: BigQueryTableSchema,
        rows: RecordBatch,
    ) -> Result<TableKey, String> {
        let dataset = DatasetKey {
            project: project.to_string(),
            dataset: QUERY_RESULTS_DATASET,
        };
        let key = BigQueryTableId::new(job_id.as_str())
            .map(|table| dataset.table(table))
            .map_err(|err| format!("the job {job_id} names no result table: {err}"))?;
        let generation = self.next_generation();
        let table = FakeTable::new(schema, vec![rows], NonZeroUsize::MIN, generation);
        self.create_table(key.clone(), table)
            .map_err(|err| format!("the result table of the job {job_id}: {err}"))?;
        Ok(key)
    }

    /// Writes a query's result into its destination `table` as the job's `write_disposition`
    /// says, creating the table in its existing dataset if it does not exist. `WRITE_TRUNCATE`
    /// replaces the table's schema and rows; `WRITE_APPEND`, and `WRITE_EMPTY` into an empty
    /// table, keep its schema and take the result as [`FakeTable::appended`] does.
    ///
    /// # Errors
    /// `NotFound` for a table whose dataset does not exist, `AlreadyExists` for `WRITE_EMPTY`
    /// into a table that holds rows, and `InvalidArgument` for a result the table's columns do
    /// not take.
    fn write_query_result(
        &mut self,
        table: &TableKey,
        write_disposition: &str,
        schema: &BigQueryTableSchema,
        rows: RecordBatch,
    ) -> Result<(), Status> {
        let generation = self.next_generation();
        let Some(existing) = self.tables.get_mut(table) else {
            if !self.datasets.contains_key(&table.dataset) {
                return Err(table.dataset.not_found());
            }
            let created = FakeTable::new(schema.clone(), vec![rows], NonZeroUsize::MIN, generation);
            return self.create_table(table.clone(), created).map_err(|err| {
                Status::internal(format!("bigquery fake: the destination table: {err}"))
            });
        };
        match write_disposition {
            "WRITE_TRUNCATE" => {
                let replaced = FakeTable::new(
                    schema.clone(),
                    vec![rows],
                    existing.read_streams,
                    generation,
                );
                existing.schema = replaced.schema;
                existing.arrow_schema = replaced.arrow_schema;
                existing.batches = replaced.batches;
            }
            "WRITE_APPEND" => {
                let appended = existing.appended(table, schema, &rows)?;
                existing.batches.push(appended);
            }
            _ if existing.num_rows() > 0 => return Err(table.already_exists()),
            _ => existing.batches = vec![existing.appended(table, schema, &rows)?],
        }
        existing.generation = generation;
        Ok(())
    }
}

impl FakeTable {
    /// `rows`, a query result of `schema`, in this table's layout, as BigQuery appends a
    /// result to a table: each result column goes to the table's column of its name, ignoring
    /// case, and a column the result leaves out is NULL. Descriptions, default values and the
    /// other column settings play no part.
    ///
    /// # Errors
    /// `InvalidArgument`, as [`append_refusal`] says, for a result the table's columns do not
    /// take.
    fn appended(
        &self,
        table: &TableKey,
        schema: &BigQueryTableSchema,
        rows: &RecordBatch,
    ) -> Result<RecordBatch, Status> {
        let refused = |problem: String| {
            Status::invalid_argument(format!(
                "Invalid schema update. Table {}: {problem}",
                table.legacy_id()
            ))
        };
        if let Some(problem) = append_refusal(&self.schema.fields, &schema.fields) {
            return Err(refused(problem));
        }
        fit_to_layout(rows, &self.arrow_schema).map_err(refused)
    }
}

/// Why columns `table` do not take a query result of the columns `result`, or `None` if they
/// do. Each result column must name a column of the table, ignoring case, of the same type, a
/// RECORD being checked field by field. A declared length, precision or scale plays no part:
/// BigQuery checks the values against it as it writes them. A REPEATED column takes only a REPEATED result, and a
/// REQUIRED one only a REQUIRED result; a REQUIRED column the result leaves out is refused.
fn append_refusal(table: &[BigQueryFieldSchema], result: &[BigQueryFieldSchema]) -> Option<String> {
    for column in result {
        let Some(target) = table
            .iter()
            .find(|target| target.name.eq_ignore_ascii_case(&column.name))
        else {
            return Some(format!("the table has no field {}", column.name));
        };
        let nested = match (&target.field_type, &column.field_type) {
            (BigQueryFieldType::Struct(target_fields), BigQueryFieldType::Struct(fields)) => {
                append_refusal(target_fields, fields)
            }
            (BigQueryFieldType::Range(target_element), BigQueryFieldType::Range(element))
                if target_element != element =>
            {
                Some(format!("field {} has changed type", column.name))
            }
            (target_type, column_type)
                if FieldKind::from(target_type) != FieldKind::from(column_type) =>
            {
                Some(format!("field {} has changed type", column.name))
            }
            _ => None,
        };
        if nested.is_some() {
            return nested;
        }
        let mode_taken = match target.mode {
            BigQueryFieldMode::Repeated => column.mode == BigQueryFieldMode::Repeated,
            BigQueryFieldMode::Required => column.mode == BigQueryFieldMode::Required,
            BigQueryFieldMode::Nullable => column.mode != BigQueryFieldMode::Repeated,
        };
        if !mode_taken {
            return Some(format!(
                "field {} has changed mode from {} to {}",
                column.name,
                target.mode.name(),
                column.mode.name()
            ));
        }
    }
    table
        .iter()
        .find(|target| {
            target.mode == BigQueryFieldMode::Required
                && !result
                    .iter()
                    .any(|column| column.name.eq_ignore_ascii_case(&target.name))
        })
        .map(|missing| {
            format!(
                "the result has no value for REQUIRED field {}",
                missing.name
            )
        })
}

impl BigQueryDmlStats {
    /// The rows the statement changed, of every kind.
    fn affected_rows(self) -> i64 {
        self.inserted + self.updated + self.deleted
    }
}

impl BigQueryJobId {
    /// The v2 `JobReference` of this job, run in `project` at `location`.
    fn reference(&self, project: &str, location: &str) -> JobReference {
        JobReference {
            project_id: project.to_string(),
            job_id: self.to_string(),
            location: Some(location.to_string()),
        }
    }

    /// The `NotFound` BigQuery answers for a job of `project` it does not find at `location`.
    fn not_found(&self, project: &str, location: &str) -> Status {
        Status::not_found(format!(
            "Not found: Job {project}:{}.{self}",
            location_or_default(location)
        ))
    }

    /// The `AlreadyExists` BigQuery answers for inserting a job whose ID is taken.
    fn already_exists(&self, project: &str, location: &str) -> Status {
        Status::already_exists(format!("Already Exists: Job {project}:{location}.{self}"))
    }
}

/// The rows of the first page: all of them unless the call's `max_results` allows fewer.
fn first_page(query: &QueryRequest, rows: &RecordBatch) -> RecordBatch {
    let page = query.max_results.map_or(rows.num_rows(), |max| {
        usize::try_from(max).map_or(rows.num_rows(), |max| max.min(rows.num_rows()))
    });
    rows.slice(0, page)
}

/// `rows` as the Arrow schema and record batch messages of a `Query` response.
///
/// # Errors
/// An internal failure of the fake if the rows do not encode as Arrow IPC.
fn arrow_results(
    rows: &RecordBatch,
    compression: IpcCompression,
) -> Result<(ResultsSchema, Results), String> {
    let batches = if rows.num_rows() == 0 {
        &[][..]
    } else {
        std::slice::from_ref(rows)
    };
    let messages = IpcMessages::encode(rows.schema_ref(), batches, compression)
        .map_err(|err| format!("the rows do not encode as Arrow IPC: {err}"))?;
    Ok((
        ResultsSchema::ArrowSchema(ArrowSchema {
            serialized_schema: messages.schema,
        }),
        // A page without rows comes as its schema and an empty record batch message.
        Results::ArrowRecordBatch(ArrowRecordBatch {
            serialized_record_batch: messages.batches.into_iter().next().unwrap_or_default(),
        }),
    ))
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

#[cfg(test)]
mod tests {
    use crate::errors::BigQueryError;
    use crate::testing::state::QUERY_RESULTS_DATASET;
    use crate::testing::{
        BigQueryFake, BigQueryFakeCode, BigQueryFakeFault, BigQueryFakeJobFailure,
    };
    use crate::{
        BigQueryDatasetId, BigQueryDmlStats, BigQueryJobId, BigQueryJobRef, BigQueryJobState,
        BigQueryJobType, BigQueryLocation, BigQueryResult, BigQuerySchemaColumns,
        BigQuerySchemaColumnsBuilder, BigQueryStatementType, BigQueryTableId,
    };
    use arrow_array::RecordBatch;
    use futures::TryStreamExt;
    use gcloud_sdk::google::cloud::bigquery::v2::{
        GetQueryResultsRequest, InsertJobRequest, Job, JobConfiguration, JobConfigurationQuery,
        JobReference, PostQueryRequest, QueryRequest,
    };
    use gcloud_sdk::tonic::Code;
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Order {
        id: i64,
        customer: String,
    }

    const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
    const TOP_ORDERS: BigQueryTableId = BigQueryTableId::from_static("top_orders");
    const BEST_ORDERS: &str = "SELECT id, customer FROM shop.orders ORDER BY total DESC LIMIT 2";
    const CLOSE_ORDERS: &str = "UPDATE shop.orders SET closed = TRUE WHERE total = 0";
    const CREATE_ARCHIVE: &str = "CREATE TABLE shop.archive (id INT64)";

    fn order(id: i64, customer: &str) -> Order {
        Order {
            id,
            customer: customer.to_string(),
        }
    }

    fn best_orders() -> Vec<Order> {
        vec![order(1, "Alice"), order(2, "Bob")]
    }

    fn invalid_query() -> BigQueryFakeJobFailure {
        BigQueryFakeJobFailure::new("invalidQuery", "Division by zero")
    }

    #[tokio::test]
    async fn dml_reports_its_counts_and_bytes() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        let stats = BigQueryDmlStats {
            inserted: 0,
            updated: 3,
            deleted: 0,
        };
        fake.when_query_match(CLOSE_ORDERS)
            .bytes_processed(4096)
            .returns_dml(BigQueryStatementType::Update, stats)?;

        let outcome = fake.db().fluent().query(CLOSE_ORDERS).execute().await?;

        assert_eq!(outcome.statement_type, Some(BigQueryStatementType::Update));
        assert_eq!(outcome.dml_stats, Some(stats));
        assert_eq!(outcome.num_dml_affected_rows, Some(3));
        assert_eq!(outcome.total_bytes_processed, Some(4096));
        Ok(())
    }

    #[tokio::test]
    async fn a_dry_run_reports_the_rows_schema_and_bytes() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        fake.when_query_match(BEST_ORDERS)
            .bytes_processed(2048)
            .returns_rows(|columns| columns.from_type::<Order>(), best_orders())?;

        let dry_run = fake.db().fluent().query(BEST_ORDERS).dry_run().await?;

        let columns: BigQuerySchemaColumns = BigQuerySchemaColumnsBuilder.from_type::<Order>();
        assert_eq!(dry_run.schema, Some(columns.table_schema()?));
        assert_eq!(dry_run.total_bytes_processed, Some(2048));
        Ok(())
    }

    #[tokio::test]
    async fn a_ddl_statement_reports_its_type_and_no_rows() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        fake.when_query_match(CREATE_ARCHIVE)
            .returns_statement(BigQueryStatementType::CreateTable)?;

        let outcome = fake.db().fluent().query(CREATE_ARCHIVE).execute().await?;

        assert_eq!(
            outcome.statement_type,
            Some(BigQueryStatementType::CreateTable)
        );
        assert_eq!((outcome.total_rows, outcome.dml_stats), (None, None));
        Ok(())
    }

    #[tokio::test]
    async fn rows_beyond_max_results_go_to_the_hidden_job_table() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        fake.when_query_match(BEST_ORDERS)
            .returns_rows(|columns| columns.from_type::<Order>(), best_orders())?;

        let outcome = fake.db().fluent().query(BEST_ORDERS).execute().await?;

        assert_eq!(outcome.total_rows, Some(2));
        let job = outcome
            .job
            .expect("a result beyond max_results runs as a job");
        let details = fake
            .db()
            .get_query_job(&job, &tracing::Span::none())
            .await?;
        let destination = details
            .configuration
            .and_then(|configuration| configuration.query)
            .and_then(|query| query.destination_table)
            .expect("the job names its result table");
        let hidden = QUERY_RESULTS_DATASET.table(BigQueryTableId::new(job.job_id.as_str())?);
        assert_eq!(destination, hidden.table_reference("fake-project"));
        assert_eq!(fake.rows::<Order>(hidden)?, best_orders());
        Ok(())
    }

    #[tokio::test]
    async fn a_destination_table_is_written_by_its_disposition() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        fake.create_dataset(SHOP)?;
        fake.when_query_match(BEST_ORDERS)
            .returns_rows(|columns| columns.from_type::<Order>(), best_orders())?;
        let into = || fake.db().fluent().query(BEST_ORDERS);

        into()
            .destination_table(SHOP.table(TOP_ORDERS))
            .execute()
            .await?;
        assert_eq!(fake.rows::<Order>(SHOP.table(TOP_ORDERS))?, best_orders());

        into()
            .append_to_destination_table(SHOP.table(TOP_ORDERS))
            .execute()
            .await?;
        let twice: Vec<Order> = best_orders().into_iter().chain(best_orders()).collect();
        assert_eq!(fake.rows::<Order>(SHOP.table(TOP_ORDERS))?, twice);

        into()
            .dangerously_overwrite_destination_table(SHOP.table(TOP_ORDERS))
            .execute()
            .await?;
        assert_eq!(fake.rows::<Order>(SHOP.table(TOP_ORDERS))?, best_orders());
        Ok(())
    }

    #[tokio::test]
    async fn a_destination_in_a_missing_dataset_is_not_found() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        fake.when_query_match(BEST_ORDERS)
            .returns_rows(|columns| columns.from_type::<Order>(), best_orders())?;

        let refused = fake
            .db()
            .fluent()
            .query(BEST_ORDERS)
            .destination_table(SHOP.table(TOP_ORDERS))
            .execute()
            .await;

        assert!(
            matches!(refused, Err(BigQueryError::DataNotFoundError(_))),
            "{refused:?}"
        );
        let table = fake.rows::<Order>(SHOP.table(TOP_ORDERS));
        assert!(
            matches!(table, Err(BigQueryError::DataNotFoundError(_))),
            "{table:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn rows_beyond_the_inline_limit_read_back_through_the_job_table() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        let orders = vec![order(1, "Alice"), order(2, "Bob"), order(3, "Carol")];
        fake.when_query_match(BEST_ORDERS)
            .returns_rows(|columns| columns.from_type::<Order>(), &orders)?;
        let query = || fake.db().fluent().query(BEST_ORDERS).inline_rows_limit(1);

        let rows: Vec<Order> = query().obj().query().await?;
        let batches: Vec<RecordBatch> = query().record_batches().await?.try_collect().await?;

        assert_eq!(rows, orders);
        let batch_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(batch_rows, orders.len());
        Ok(())
    }

    #[tokio::test]
    async fn write_empty_into_a_table_with_rows_is_a_data_conflict() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        let held = vec![order(9, "Carol")];
        fake.table(SHOP.table(TOP_ORDERS), |columns| {
            columns.from_type::<Order>()
        })
        .rows(&held)
        .create()?;
        fake.when_query_match(BEST_ORDERS)
            .returns_rows(|columns| columns.from_type::<Order>(), best_orders())?;

        let refused = fake
            .db()
            .fluent()
            .query(BEST_ORDERS)
            .destination_table(SHOP.table(TOP_ORDERS))
            .execute()
            .await;

        assert!(
            matches!(refused, Err(BigQueryError::DataConflictError(_))),
            "{refused:?}"
        );
        assert_eq!(fake.rows::<Order>(SHOP.table(TOP_ORDERS))?, held);
        Ok(())
    }

    #[tokio::test]
    async fn get_job_reads_a_finished_query_job() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        fake.when_query_match(BEST_ORDERS)
            .bytes_processed(512)
            .returns_rows(|columns| columns.from_type::<Order>(), best_orders())?;

        let (_, stats) = fake
            .db()
            .fluent()
            .query(BEST_ORDERS)
            .job_creation_required()
            .obj::<Order>()
            .query_with_stats()
            .await?;
        let job = stats.job.expect("a required job");
        let read = fake.db().get_job(&job).await?;

        assert_eq!(read.reference, job);
        assert_eq!(read.job_type, Some(BigQueryJobType::Query));
        assert_eq!(read.state, Some(BigQueryJobState::Done));
        assert_eq!(read.statement_type, Some(BigQueryStatementType::Select));
        assert_eq!(read.total_bytes_processed, Some(512));
        assert_eq!(read.error, None);
        Ok(())
    }

    #[tokio::test]
    async fn an_unknown_job_is_not_found() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        let missing = BigQueryJobRef {
            project_id: "fake-project".to_string(),
            job_id: BigQueryJobId::new("missing-job")?,
            location: None,
        };

        let read = fake.db().get_job(&missing).await;

        assert!(
            matches!(read, Err(BigQueryError::DataNotFoundError(_))),
            "{read:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancel_job_stops_a_running_job() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        fake.when_query_match(BEST_ORDERS)
            .fails_job(invalid_query())?;
        let response = fake
            .db()
            .job_client()
            .query(PostQueryRequest {
                project_id: "fake-project".to_string(),
                query_request: Some(QueryRequest {
                    query: BEST_ORDERS.to_string(),
                    ..Default::default()
                }),
            })
            .await
            .map_err(BigQueryError::from)?
            .into_inner();
        assert_eq!(response.job_complete, Some(false));
        let job = BigQueryJobRef::from(response.job_reference.expect("a running job"));

        fake.db().cancel_job(&job).await?;
        let read = fake.db().get_job(&job).await?;

        assert_eq!(read.state, Some(BigQueryJobState::Done));
        assert_eq!(
            read.error.map(|error| error.reason),
            Some("stopped".to_string())
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancelling_a_finished_job_leaves_it_as_it_finished() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        fake.when_query_match(CLOSE_ORDERS)
            .returns_statement(BigQueryStatementType::Update)?;
        let outcome = fake
            .db()
            .fluent()
            .query(CLOSE_ORDERS)
            .job_creation_required()
            .execute()
            .await?;
        let job = outcome.job.expect("a required job");

        fake.db().cancel_job(&job).await?;
        let read = fake.db().get_job(&job).await?;

        assert_eq!(read.state, Some(BigQueryJobState::Done));
        assert_eq!(read.error, None);
        Ok(())
    }

    #[tokio::test]
    async fn a_fault_answers_get_job_before_the_job_records() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        let fault = fake
            .when_fault(crate::testing::BigQueryFakeRpc::GetJob)
            .times(1)
            .fails(BigQueryFakeFault::status(
                BigQueryFakeCode::PermissionDenied,
                "no access",
            ))?;
        let missing = BigQueryJobRef {
            project_id: "fake-project".to_string(),
            job_id: BigQueryJobId::new("missing-job")?,
            location: None,
        };

        let read = fake.db().get_job(&missing).await;

        assert!(
            matches!(&read, Err(BigQueryError::DatabaseError(err)) if err.public.code == "PermissionDenied"),
            "{read:?}"
        );
        assert_eq!(fault.calls(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn a_job_named_in_another_location_is_not_found() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        fake.when_query_match(CLOSE_ORDERS)
            .returns_statement(BigQueryStatementType::Update)?;
        let outcome = fake
            .db()
            .fluent()
            .query(CLOSE_ORDERS)
            .job_creation_required()
            .execute()
            .await?;
        let job = outcome.job.expect("a required job");
        let elsewhere = BigQueryJobRef {
            location: Some(BigQueryLocation::from_static("EU")),
            ..job.clone()
        };

        let read = fake.db().get_job(&elsewhere).await.map(drop);
        let cancelled = fake.db().cancel_job(&elsewhere).await;
        let waited = fake
            .db()
            .job_client()
            .get_query_results(GetQueryResultsRequest {
                project_id: job.project_id.clone(),
                job_id: job.job_id.to_string(),
                location: "EU".to_string(),
                ..Default::default()
            })
            .await
            .map(drop)
            .map_err(BigQueryError::from);

        for answer in [read, cancelled, waited] {
            assert!(
                matches!(answer, Err(BigQueryError::DataNotFoundError(_))),
                "{answer:?}"
            );
        }
        Ok(())
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Customer {
        name: String,
    }

    #[tokio::test]
    async fn write_empty_into_an_empty_table_of_other_columns_is_refused() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        fake.table(SHOP.table(TOP_ORDERS), |columns| {
            columns.from_type::<Customer>()
        })
        .create()?;
        fake.when_query_match(BEST_ORDERS)
            .returns_rows(|columns| columns.from_type::<Order>(), best_orders())?;

        let refused = fake
            .db()
            .fluent()
            .query(BEST_ORDERS)
            .destination_table(SHOP.table(TOP_ORDERS))
            .execute()
            .await;

        assert!(
            matches!(&refused, Err(err) if err.has_code(Code::InvalidArgument)),
            "{refused:?}"
        );
        assert_eq!(fake.rows::<Customer>(SHOP.table(TOP_ORDERS))?, Vec::new());
        Ok(())
    }

    #[tokio::test]
    async fn an_append_ignores_descriptions_and_takes_required_into_nullable() -> BigQueryResult<()>
    {
        let fake = BigQueryFake::start().await?;
        fake.table(SHOP.table(TOP_ORDERS), |columns| {
            columns.from_type::<Order>().with("customer", |customer| {
                customer.nullable().description("who ordered")
            })
        })
        .create()?;
        fake.when_query_match(BEST_ORDERS)
            .returns_rows(|columns| columns.from_type::<Order>(), best_orders())?;

        fake.db()
            .fluent()
            .query(BEST_ORDERS)
            .append_to_destination_table(SHOP.table(TOP_ORDERS))
            .execute()
            .await?;

        assert_eq!(fake.rows::<Order>(SHOP.table(TOP_ORDERS))?, best_orders());
        Ok(())
    }

    const DELIVERIES: BigQueryTableId = BigQueryTableId::from_static("deliveries");
    const RECENT_DELIVERIES: &str = "SELECT id, address FROM shop.deliveries LIMIT 1";

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Address {
        city: String,
        zip: Option<String>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Delivery {
        id: i64,
        address: Address,
    }

    fn paris_delivery() -> Delivery {
        Delivery {
            id: 1,
            address: Address {
                city: "Paris".to_string(),
                zip: Some("75001".to_string()),
            },
        }
    }

    #[tokio::test]
    async fn an_append_takes_a_required_nested_field_into_a_nullable_one() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        fake.table(SHOP.table(DELIVERIES), |columns| {
            columns
                .from_type::<Delivery>()
                .with("address.city", |city| city.nullable())
        })
        .create()?;
        fake.when_query_match(RECENT_DELIVERIES).returns_rows(
            |columns| columns.from_type::<Delivery>(),
            [paris_delivery()],
        )?;

        fake.db()
            .fluent()
            .query(RECENT_DELIVERIES)
            .append_to_destination_table(SHOP.table(DELIVERIES))
            .execute()
            .await?;

        assert_eq!(
            fake.rows::<Delivery>(SHOP.table(DELIVERIES))?,
            vec![paris_delivery()]
        );
        Ok(())
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct ShoutedAddress {
        #[serde(rename = "ZIP")]
        zip: Option<String>,
        #[serde(rename = "City")]
        city: String,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct ShoutedDelivery {
        id: i64,
        #[serde(rename = "ADDRESS")]
        address: ShoutedAddress,
    }

    #[tokio::test]
    async fn an_append_matches_nested_fields_by_name_ignoring_case_and_order() -> BigQueryResult<()>
    {
        let fake = BigQueryFake::start().await?;
        fake.table(SHOP.table(DELIVERIES), |columns| {
            columns.from_type::<Delivery>()
        })
        .create()?;
        let shouted = ShoutedDelivery {
            id: 1,
            address: ShoutedAddress {
                zip: Some("75001".to_string()),
                city: "Paris".to_string(),
            },
        };
        fake.when_query_match(RECENT_DELIVERIES)
            .returns_rows(|columns| columns.from_type::<ShoutedDelivery>(), [shouted])?;

        fake.db()
            .fluent()
            .query(RECENT_DELIVERIES)
            .append_to_destination_table(SHOP.table(DELIVERIES))
            .execute()
            .await?;

        assert_eq!(
            fake.rows::<Delivery>(SHOP.table(DELIVERIES))?,
            vec![paris_delivery()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn an_append_takes_a_string_into_a_string_of_declared_length() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        fake.table(SHOP.table(TOP_ORDERS), |columns| {
            columns
                .from_type::<Order>()
                .with("customer", |customer| customer.string_with_max_length(10))
        })
        .create()?;
        fake.when_query_match(BEST_ORDERS)
            .returns_rows(|columns| columns.from_type::<Order>(), best_orders())?;

        fake.db()
            .fluent()
            .query(BEST_ORDERS)
            .append_to_destination_table(SHOP.table(TOP_ORDERS))
            .execute()
            .await?;

        assert_eq!(fake.rows::<Order>(SHOP.table(TOP_ORDERS))?, best_orders());
        Ok(())
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Payment {
        id: i64,
        amount: String,
    }

    const PAYMENTS: BigQueryTableId = BigQueryTableId::from_static("payments");
    const LARGE_PAYMENTS: &str = "SELECT id, amount FROM shop.ledger WHERE amount > 1";

    fn payment(id: i64, amount: &str) -> Payment {
        Payment {
            id,
            amount: amount.to_string(),
        }
    }

    /// A fake whose `shop.payments` declares `amount` NUMERIC(10, 2), and whose query
    /// [`LARGE_PAYMENTS`] returns `payments` with `amount` as plain NUMERIC.
    async fn fake_with_large_payments(payments: Vec<Payment>) -> BigQueryResult<BigQueryFake> {
        let fake = BigQueryFake::start().await?;
        fake.table(SHOP.table(PAYMENTS), |columns| {
            columns
                .from_type::<Payment>()
                .with("amount", |amount| amount.numeric_with(10, 2))
        })
        .create()?;
        fake.when_query_match(LARGE_PAYMENTS).returns_rows(
            |columns| {
                columns
                    .from_type::<Payment>()
                    .with("amount", |amount| amount.numeric())
            },
            payments,
        )?;
        Ok(fake)
    }

    #[tokio::test]
    async fn an_append_rounds_a_numeric_to_the_declared_scale() -> BigQueryResult<()> {
        let fake =
            fake_with_large_payments(vec![payment(1, "1.505"), payment(2, "-2.004")]).await?;

        fake.db()
            .fluent()
            .query(LARGE_PAYMENTS)
            .append_to_destination_table(SHOP.table(PAYMENTS))
            .execute()
            .await?;

        assert_eq!(
            fake.rows::<Payment>(SHOP.table(PAYMENTS))?,
            vec![payment(1, "1.51"), payment(2, "-2")]
        );
        Ok(())
    }

    #[tokio::test]
    async fn an_append_refuses_a_numeric_beyond_the_declared_precision() -> BigQueryResult<()> {
        let fake = fake_with_large_payments(vec![payment(1, "123456789.5")]).await?;

        let refused = fake
            .db()
            .fluent()
            .query(LARGE_PAYMENTS)
            .append_to_destination_table(SHOP.table(PAYMENTS))
            .execute()
            .await;

        assert!(
            matches!(&refused, Err(err) if err.has_code(Code::InvalidArgument)),
            "{refused:?}"
        );
        assert_eq!(fake.rows::<Payment>(SHOP.table(PAYMENTS))?, Vec::new());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_insert_jobs_of_one_id_create_it_once() -> BigQueryResult<()> {
        let fake = BigQueryFake::start().await?;
        let first = AtomicBool::new(true);
        // The first call holds the rules while the second passes its own checks, so both
        // are in flight at once.
        fake.when_query(move |query| {
            if first.swap(false, Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(300));
            }
            query.sql() == CLOSE_ORDERS
        })
        .returns_statement(BigQueryStatementType::Update)?;
        let insert = || async {
            let mut jobs = fake.db().job_client();
            jobs.insert_job(InsertJobRequest {
                project_id: "fake-project".to_string(),
                job: Some(Job {
                    job_reference: Some(JobReference {
                        project_id: "fake-project".to_string(),
                        job_id: "close-orders".to_string(),
                        location: None,
                    }),
                    configuration: Some(JobConfiguration {
                        query: Some(JobConfigurationQuery {
                            query: CLOSE_ORDERS.to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            })
            .await
        };

        let (one, other) = tokio::join!(insert(), insert());

        let refused: Vec<Code> = [one, other]
            .into_iter()
            .filter_map(Result::err)
            .map(|status| status.code())
            .collect();
        assert_eq!(refused, [Code::AlreadyExists]);
        Ok(())
    }
}
