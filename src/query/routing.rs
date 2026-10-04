//! Running a query and deciding where its rows come from.
//!
//! Every query goes through `JobService.Query` with an Arrow result format. A result that is
//! complete and whole in that first response is decoded from its inline Arrow. Anything else
//! is read from the job's destination table through the Storage Read API, since
//! `GetQueryResults` has no Arrow form and is used only to wait for the job.
//!
//! Job creation is optional unless the caller requires it, so a short query is answered
//! without a job. Whenever the first response leaves anything for a later call, BigQuery has
//! created a job for it, and that job is what the rest of the route reads.

use crate::errors::{BigQueryError, BigQueryErrorPublicGenericDetails, BigQuerySystemError};
use crate::query::jobs::{get_job, wait_for_job};
use crate::query::params::parameter_mode;
use crate::read::ArrowIpcDecoder;
use crate::{
    BigQueryDb, BigQueryDryRunResult, BigQueryJobCreation, BigQueryJobRef, BigQueryJobStats,
    BigQueryLocation, BigQueryQueryId, BigQueryQueryOutcome, BigQueryQueryParams,
    BigQueryReadCompression, BigQueryResult, BigQueryStatementType, BigQueryTableRef,
    BigQueryTableSchema,
};
use arrow_array::RecordBatch;
use gcloud_sdk::google::cloud::bigquery::v2::arrow_serialization_options::CompressionCodec;
use gcloud_sdk::google::cloud::bigquery::v2::query_request::{
    JobCreationMode, QueryResultsFormat, ResultsFormatSerializationOptions,
};
use gcloud_sdk::google::cloud::bigquery::v2::query_response::{Results, ResultsSchema};
use gcloud_sdk::google::cloud::bigquery::v2::{
    ArrowSerializationOptions, DataFormatOptions, DatasetReference, GetQueryResultsResponse, Job,
    PostQueryRequest, QueryRequest, QueryResponse,
};
use gcloud_sdk::tonic::metadata::MetadataMap;
use rand::RngExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tracing::field::Empty;
use tracing::Span;

/// How long `Query` waits for the job by default, BigQuery's own default.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// The span of one query terminal call.
pub(crate) fn query_span(params: &BigQueryQueryParams) -> Span {
    tracing::debug_span!(
        "BigQuery Query",
        "/bigquery/sql_len" = params.sql.len(),
        "/bigquery/job_id" = Empty,
        "/bigquery/query_id" = Empty,
        "/bigquery/location" = Empty,
        "/bigquery/statement_type" = Empty,
        "/bigquery/bytes_processed" = Empty,
        "/bigquery/bytes_billed" = Empty,
        "/bigquery/slot_ms" = Empty,
        "/bigquery/cache_hit" = Empty,
        "/bigquery/dml_rows" = Empty,
        "/bigquery/total_rows" = Empty,
        "/bigquery/route" = Empty,
    )
}

impl From<BigQueryJobCreation> for JobCreationMode {
    fn from(mode: BigQueryJobCreation) -> Self {
        match mode {
            BigQueryJobCreation::Optional => Self::JobCreationOptional,
            BigQueryJobCreation::Required => Self::JobCreationRequired,
        }
    }
}

/// The figures of the `Query` response; a figure it leaves out is filled later from the
/// responses the query goes on to receive.
impl From<&QueryResponse> for BigQueryJobStats {
    fn from(response: &QueryResponse) -> Self {
        let job = response.job_reference.clone().map(|reference| {
            let mut job = BigQueryJobRef::from(reference);
            if job.location.is_none() {
                job.location = BigQueryLocation::reported(response.location.clone());
            }
            job
        });
        Self {
            job,
            query_id: non_empty(response.query_id.clone()).map(BigQueryQueryId::reported),
            statement_type: non_empty(response.statement_type.clone()).map(Into::into),
            total_rows: response.total_rows,
            total_bytes_processed: response.total_bytes_processed,
            total_bytes_billed: response.total_bytes_billed,
            total_slot_ms: response.total_slot_ms,
            cache_hit: response.cache_hit,
            num_dml_affected_rows: response.num_dml_affected_rows,
            dml_stats: response.dml_stats.map(Into::into),
        }
    }
}

impl BigQueryJobStats {
    /// Fills the figures still missing from the `GetQueryResults` that saw the job complete.
    fn merge_results(&mut self, results: &GetQueryResultsResponse) {
        self.total_rows = self.total_rows.or(results.total_rows);
        self.total_bytes_processed = self.total_bytes_processed.or(results.total_bytes_processed);
        self.cache_hit = self.cache_hit.or(results.cache_hit);
        self.num_dml_affected_rows = self.num_dml_affected_rows.or(results.num_dml_affected_rows);
    }

    /// Fills the figures still missing from the finished job's statistics, the query
    /// statistics before the job-wide ones.
    fn merge_job(&mut self, job: &Job) {
        let Some(statistics) = &job.statistics else {
            return;
        };
        if let Some(query) = &statistics.query {
            self.statement_type = self
                .statement_type
                .take()
                .or_else(|| non_empty(query.statement_type.clone()).map(Into::into));
            self.total_bytes_processed = self.total_bytes_processed.or(query.total_bytes_processed);
            self.total_bytes_billed = self.total_bytes_billed.or(query.total_bytes_billed);
            self.total_slot_ms = self.total_slot_ms.or(query.total_slot_ms);
            self.cache_hit = self.cache_hit.or(query.cache_hit);
            self.num_dml_affected_rows = self.num_dml_affected_rows.or(query.num_dml_affected_rows);
            self.dml_stats = self.dml_stats.or(query.dml_stats.map(Into::into));
        }
        self.total_bytes_processed = self
            .total_bytes_processed
            .or(statistics.total_bytes_processed);
        self.total_slot_ms = self.total_slot_ms.or(statistics.total_slot_ms);
    }

    /// The figures of the first `Query` response, recorded on the query's span. A query that
    /// ran without a job has its location only in the response, not in a job reference.
    fn first_response(response: &QueryResponse, span: &Span) -> Self {
        let stats = Self::from(response);
        stats.record(span);
        if stats.job.is_none() && !response.location.is_empty() {
            span.record("/bigquery/location", response.location.as_str());
        }
        stats
    }

    /// Records the figures known so far on the query's span.
    fn record(&self, span: &Span) {
        if let Some(query_id) = &self.query_id {
            span.record("/bigquery/query_id", query_id.as_str());
        }
        if let Some(job) = &self.job {
            span.record("/bigquery/job_id", job.job_id.as_str());
            if let Some(location) = &job.location {
                span.record("/bigquery/location", location.as_str());
            }
        }
        if let Some(statement_type) = &self.statement_type {
            span.record("/bigquery/statement_type", statement_type.as_str());
        }
        if let Some(bytes) = self.total_bytes_processed {
            span.record("/bigquery/bytes_processed", bytes);
        }
        if let Some(bytes) = self.total_bytes_billed {
            span.record("/bigquery/bytes_billed", bytes);
        }
        if let Some(slot_ms) = self.total_slot_ms {
            span.record("/bigquery/slot_ms", slot_ms);
        }
        if let Some(cache_hit) = self.cache_hit {
            span.record("/bigquery/cache_hit", cache_hit);
        }
        if let Some(rows) = self.num_dml_affected_rows {
            span.record("/bigquery/dml_rows", rows);
        }
        if let Some(rows) = self.total_rows {
            span.record("/bigquery/total_rows", rows);
        }
    }
}

/// What a terminal call asks of the `Query` request.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Purpose {
    /// The rows, inline when they fit.
    Rows,
    /// What the statement did, no rows.
    Execute,
    /// The cost estimate of a dry run.
    DryRun,
}

/// Where the rows of a finished query are.
pub(crate) enum Rows {
    /// The whole result, from the first response; `None` when it has no rows.
    Inline(Option<RecordBatch>),
    /// The job's destination table, for the Storage Read API.
    Table(BigQueryTableRef),
    /// A statement with no result, such as DML or DDL.
    None,
}

fn system_error(code: &str, message: String) -> BigQueryError {
    BigQueryError::SystemError(BigQuerySystemError::new(
        BigQueryErrorPublicGenericDetails::new(code.into()),
        message,
    ))
}

fn millis_u32(name: &str, d: Duration) -> BigQueryResult<u32> {
    u32::try_from(d.as_millis()).map_err(|_| {
        BigQueryError::invalid_parameters(name, format!("{d:?} is longer than {} ms", u32::MAX))
    })
}

fn millis_i64(name: &str, d: Duration) -> BigQueryResult<i64> {
    i64::try_from(d.as_millis()).map_err(|_| {
        BigQueryError::invalid_parameters(name, format!("{d:?} is longer than {} ms", i64::MAX))
    })
}

fn non_empty(s: String) -> Option<String> {
    (!s.is_empty()).then_some(s)
}

/// A fresh idempotency key: 128 random bits in hex.
fn random_request_id() -> String {
    format!("{:032x}", rand::rng().random::<u128>())
}

fn query_request(
    db: &BigQueryDb,
    params: &BigQueryQueryParams,
    purpose: Purpose,
) -> BigQueryResult<PostQueryRequest> {
    let project_id = db.options().google_project_id.clone();
    let default_dataset = params.default_dataset.as_ref().map(|d| DatasetReference {
        project_id: d.project().unwrap_or(&project_id).to_string(),
        dataset_id: d.dataset().to_string(),
    });
    let compression = match params.read_options.compression {
        BigQueryReadCompression::None => CompressionCodec::CompressionUnspecified,
        BigQueryReadCompression::Lz4 => CompressionCodec::Lz4Frame,
        BigQueryReadCompression::Zstd => CompressionCodec::Zstd,
    };
    let max_results = match purpose {
        Purpose::Rows => params.inline_rows_limit,
        Purpose::Execute => Some(0),
        Purpose::DryRun => None,
    };
    Ok(PostQueryRequest {
        project_id,
        query_request: Some(QueryRequest {
            query: params.sql.clone(),
            max_results,
            default_dataset,
            timeout_ms: Some(millis_u32(
                "timeout",
                params.timeout.unwrap_or(DEFAULT_TIMEOUT),
            )?),
            job_timeout_ms: params
                .job_timeout
                .map(|d| millis_i64("job_timeout", d))
                .transpose()?,
            dry_run: purpose == Purpose::DryRun,
            use_query_cache: params.use_query_cache,
            use_legacy_sql: Some(false),
            parameter_mode: parameter_mode(&params.query_parameters)?.to_string(),
            query_parameters: params.query_parameters.clone(),
            location: params
                .location
                .as_ref()
                .or(db.options().location.as_ref())
                .map(ToString::to_string)
                .unwrap_or_default(),
            format_options: Some(DataFormatOptions {
                use_int64_timestamp: true,
                ..Default::default()
            }),
            labels: params.labels.clone().into_iter().collect(),
            maximum_bytes_billed: params.maximum_bytes_billed,
            job_creation_mode: JobCreationMode::from(params.job_creation).into(),
            request_id: params
                .request_id
                .as_ref()
                .map_or_else(random_request_id, ToString::to_string),
            query_results_format: QueryResultsFormat::Arrow.into(),
            results_format_serialization_options: Some(
                ResultsFormatSerializationOptions::ArrowSerializationOptions(
                    ArrowSerializationOptions {
                        buffer_compression: compression.into(),
                        ..Default::default()
                    },
                ),
            ),
            ..Default::default()
        }),
    })
}

/// Sends `Query`. Its retries are safe because every attempt repeats one `request_id`.
///
/// A retry requires a job whatever the first attempt asked for: BigQuery answers a repeated
/// `request_id` with the job the first attempt created only in that mode, and with
/// `AlreadyExists` for that job in optional mode.
async fn post_query(
    db: &BigQueryDb,
    params: &BigQueryQueryParams,
    purpose: Purpose,
    span: &Span,
) -> BigQueryResult<QueryResponse> {
    let request = query_request(db, params, purpose)?;
    let retrying = AtomicBool::new(false);
    db.retry(
        span,
        "run a query",
        &request,
        &MetadataMap::new(),
        |mut r| {
            if retrying.swap(true, Ordering::Relaxed) {
                if let Some(query) = r.get_mut().query_request.as_mut() {
                    query.job_creation_mode = JobCreationMode::JobCreationRequired.into();
                }
            }
            let mut client = db.job_client();
            async move { client.query(r).await }
        },
    )
    .await
}

fn timeout_ms(params: &BigQueryQueryParams) -> BigQueryResult<u32> {
    millis_u32("timeout", params.timeout.unwrap_or(DEFAULT_TIMEOUT))
}

/// BigQuery creates a job for any query it cannot answer whole in the first response, so a
/// response that leaves rows or the job's completion to a later call names that job.
fn missing_job() -> BigQueryError {
    system_error(
        "NO_JOB_REFERENCE",
        "BigQuery answered a query that is unfinished or only partly inline without a job \
         reference to read the rest from"
            .into(),
    )
}

/// The inline Arrow result of a `Query` response, if it carries one.
fn inline_batch(response: &QueryResponse) -> BigQueryResult<Option<RecordBatch>> {
    let (Some(ResultsSchema::ArrowSchema(schema)), Some(Results::ArrowRecordBatch(batch))) =
        (&response.results_schema, &response.results)
    else {
        return Ok(None);
    };
    // A result with no rows comes as the schema and a record batch message of zero bytes.
    if batch.serialized_record_batch.is_empty() {
        return Ok(None);
    }
    ArrowIpcDecoder::new(&schema.serialized_schema)?
        .decode(&batch.serialized_record_batch)
        .map(Some)
}

/// Runs the query and finds its rows, with the figures of every response it received.
pub(crate) async fn query_rows(
    db: &BigQueryDb,
    params: &BigQueryQueryParams,
    span: &Span,
) -> BigQueryResult<(Rows, BigQueryJobStats)> {
    let response = post_query(db, params, Purpose::Rows, span).await?;
    let mut stats = BigQueryJobStats::first_response(&response, span);
    if response.job_complete == Some(true) {
        if response.page_token.is_empty() {
            let batch = inline_batch(&response)?;
            let inline_rows = batch.as_ref().map_or(0, |b| b.num_rows() as u64);
            // DML and DDL report no total; a SELECT reports one, and the inline page is the
            // whole result only when it holds that many rows.
            if response.total_rows.is_none_or(|total| total == inline_rows) {
                span.record("/bigquery/route", "inline");
                return Ok((Rows::Inline(batch), stats));
            }
        }
    } else {
        let job = stats.job.as_ref().ok_or_else(missing_job)?;
        let results = wait_for_job(db, job, timeout_ms(params)?, span).await?;
        stats.merge_results(&results);
    }
    let job = stats.job.clone().ok_or_else(missing_job)?;
    let details = get_job(db, &job, span).await?;
    stats.merge_job(&details);
    stats.record(span);
    let destination = details
        .configuration
        .as_ref()
        .and_then(|c| c.query.as_ref())
        .and_then(|q| q.destination_table.clone());
    match destination {
        Some(table) => {
            span.record("/bigquery/route", "storage_read");
            let table = BigQueryTableRef::try_from(table).map_err(|err| {
                system_error(
                    "INVALID_DESTINATION_TABLE",
                    format!(
                        "The query job {} names a destination table this crate cannot read: {err}",
                        job.job_id
                    ),
                )
            })?;
            Ok((Rows::Table(table), stats))
        }
        None if statement_type_of(&details) == Some(BigQueryStatementType::Select) => {
            Err(system_error(
                "NO_DESTINATION_TABLE",
                format!(
                    "The query job {} returned rows but has no destination table to read them from",
                    job.job_id
                ),
            ))
        }
        None => {
            span.record("/bigquery/route", "none");
            Ok((Rows::None, stats))
        }
    }
}

fn statement_type_of(job: &Job) -> Option<BigQueryStatementType> {
    job.statistics
        .as_ref()
        .and_then(|s| s.query.as_ref())
        .and_then(|q| non_empty(q.statement_type.clone()))
        .map(Into::into)
}

/// Runs the statement, waits for it, and reports what it did.
pub(crate) async fn execute(
    db: &BigQueryDb,
    params: &BigQueryQueryParams,
    span: &Span,
) -> BigQueryResult<BigQueryQueryOutcome> {
    let response = post_query(db, params, Purpose::Execute, span).await?;
    let mut stats = BigQueryJobStats::first_response(&response, span);
    if response.job_complete == Some(true) {
        return Ok(stats.into());
    }
    let job = stats.job.clone().ok_or_else(missing_job)?;
    let results = wait_for_job(db, &job, timeout_ms(params)?, span).await?;
    stats.merge_results(&results);
    let details = get_job(db, &job, span).await?;
    stats.merge_job(&details);
    stats.record(span);
    Ok(stats.into())
}

/// Validates the statement and reports its cost without running it.
pub(crate) async fn dry_run(
    db: &BigQueryDb,
    params: &BigQueryQueryParams,
    span: &Span,
) -> BigQueryResult<BigQueryDryRunResult> {
    let response = post_query(db, params, Purpose::DryRun, span).await?;
    Ok(BigQueryDryRunResult {
        total_bytes_processed: response.total_bytes_processed,
        schema: response
            .schema
            .as_ref()
            .map(BigQueryTableSchema::try_from)
            .transpose()?,
    })
}
