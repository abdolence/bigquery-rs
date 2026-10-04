//! Running a query and deciding where its rows come from.
//!
//! Every query goes through `JobService.Query` with an Arrow result format. A result that is
//! complete and whole in that first response is decoded from its inline Arrow. Anything else
//! is read from the job's destination table through the Storage Read API, since
//! `GetQueryResults` has no Arrow form and is used only to wait for the job.

use crate::errors::{BigQueryError, BigQueryErrorPublicGenericDetails, BigQuerySystemError};
use crate::query::jobs::{get_job, wait_for_job};
use crate::query::params::parameter_mode;
use crate::read::ArrowIpcDecoder;
use crate::{
    BigQueryDb, BigQueryDryRunResult, BigQueryJobRef, BigQueryQueryOutcome, BigQueryQueryParams,
    BigQueryReadCompression, BigQueryResult, BigQueryTableRef, BigQueryTableSchema,
};
use arrow_array::RecordBatch;
use gcloud_sdk::google::cloud::bigquery::v2::arrow_serialization_options::CompressionCodec;
use gcloud_sdk::google::cloud::bigquery::v2::query_request::{
    QueryResultsFormat, ResultsFormatSerializationOptions,
};
use gcloud_sdk::google::cloud::bigquery::v2::query_response::{Results, ResultsSchema};
use gcloud_sdk::google::cloud::bigquery::v2::{
    ArrowSerializationOptions, DataFormatOptions, DatasetReference, Job, PostQueryRequest,
    QueryRequest, QueryResponse,
};
use gcloud_sdk::tonic::metadata::MetadataMap;
use rand::RngExt;
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
        "/bigquery/route" = Empty,
        "/bigquery/total_rows" = Empty,
    )
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
    let default_dataset = params
        .default_dataset
        .as_ref()
        .map(|d| match d.split_once('.') {
            Some((project, dataset)) => DatasetReference {
                project_id: project.to_string(),
                dataset_id: dataset.to_string(),
            },
            None => DatasetReference {
                project_id: project_id.clone(),
                dataset_id: d.clone(),
            },
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
                .clone()
                .or_else(|| db.options().location.clone())
                .unwrap_or_default(),
            format_options: Some(DataFormatOptions {
                use_int64_timestamp: true,
                ..Default::default()
            }),
            labels: params.labels.clone().into_iter().collect(),
            maximum_bytes_billed: params.maximum_bytes_billed,
            request_id: params.request_id.clone().unwrap_or_else(random_request_id),
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
async fn post_query(
    db: &BigQueryDb,
    params: &BigQueryQueryParams,
    purpose: Purpose,
    span: &Span,
) -> BigQueryResult<QueryResponse> {
    let request = query_request(db, params, purpose)?;
    db.retry(span, "run a query", &request, &MetadataMap::new(), |r| {
        let mut client = db.job_client();
        async move { client.query(r).await }
    })
    .await
}

fn timeout_ms(params: &BigQueryQueryParams) -> BigQueryResult<u32> {
    millis_u32("timeout", params.timeout.unwrap_or(DEFAULT_TIMEOUT))
}

/// The job of a `Query` response, located where BigQuery said it ran.
fn response_job(response: &QueryResponse, span: &Span) -> Option<BigQueryJobRef> {
    let mut job = BigQueryJobRef::from(response.job_reference.clone()?);
    if job.location.is_none() {
        job.location = non_empty(response.location.clone());
    }
    span.record("/bigquery/job_id", job.job_id.as_str());
    Some(job)
}

fn missing_job() -> BigQueryError {
    system_error(
        "NO_JOB_REFERENCE",
        "BigQuery answered an unfinished query without a job reference to wait on".into(),
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

/// Runs the query and finds its rows.
pub(crate) async fn query_rows(
    db: &BigQueryDb,
    params: &BigQueryQueryParams,
    span: &Span,
) -> BigQueryResult<Rows> {
    let response = post_query(db, params, Purpose::Rows, span).await?;
    let job = response_job(&response, span);
    if response.job_complete == Some(true) {
        if let Some(total) = response.total_rows {
            span.record("/bigquery/total_rows", total);
        }
        if response.page_token.is_empty() {
            let batch = inline_batch(&response)?;
            let inline_rows = batch.as_ref().map_or(0, |b| b.num_rows() as u64);
            // DML and DDL report no total; a SELECT reports one, and the inline page is the
            // whole result only when it holds that many rows.
            if response.total_rows.is_none_or(|total| total == inline_rows) {
                span.record("/bigquery/route", "inline");
                return Ok(Rows::Inline(batch));
            }
        }
    } else {
        let job = job.as_ref().ok_or_else(missing_job)?;
        wait_for_job(db, job, timeout_ms(params)?, span).await?;
    }
    let job = job.ok_or_else(missing_job)?;
    let details = get_job(db, &job, span).await?;
    let destination = details
        .configuration
        .as_ref()
        .and_then(|c| c.query.as_ref())
        .and_then(|q| q.destination_table.clone());
    match destination {
        Some(table) => {
            span.record("/bigquery/route", "storage_read");
            Ok(Rows::Table(BigQueryTableRef::from((
                table.project_id,
                table.dataset_id,
                table.table_id,
            ))))
        }
        None if statement_type_of(&details).as_deref() == Some("SELECT") => Err(system_error(
            "NO_DESTINATION_TABLE",
            format!(
                "The query job {} returned rows but has no destination table to read them from",
                job.job_id
            ),
        )),
        None => {
            span.record("/bigquery/route", "none");
            Ok(Rows::None)
        }
    }
}

fn statement_type_of(job: &Job) -> Option<String> {
    job.statistics
        .as_ref()
        .and_then(|s| s.query.as_ref())
        .and_then(|q| non_empty(q.statement_type.clone()))
}

/// Runs the statement, waits for it, and reports what it did.
pub(crate) async fn execute(
    db: &BigQueryDb,
    params: &BigQueryQueryParams,
    span: &Span,
) -> BigQueryResult<BigQueryQueryOutcome> {
    let response = post_query(db, params, Purpose::Execute, span).await?;
    let job = response_job(&response, span);
    if response.job_complete == Some(true) {
        return Ok(BigQueryQueryOutcome {
            job,
            statement_type: non_empty(response.statement_type),
            num_dml_affected_rows: response.num_dml_affected_rows,
            dml_stats: response.dml_stats.map(Into::into),
            total_rows: response.total_rows,
            total_bytes_processed: response.total_bytes_processed,
            cache_hit: response.cache_hit,
        });
    }
    let job = job.ok_or_else(missing_job)?;
    let results = wait_for_job(db, &job, timeout_ms(params)?, span).await?;
    let details = get_job(db, &job, span).await?;
    let statistics = details.statistics.and_then(|s| s.query).unwrap_or_default();
    Ok(BigQueryQueryOutcome {
        job: Some(job),
        statement_type: non_empty(statistics.statement_type),
        num_dml_affected_rows: results
            .num_dml_affected_rows
            .or(statistics.num_dml_affected_rows),
        dml_stats: statistics.dml_stats.map(Into::into),
        total_rows: results.total_rows,
        total_bytes_processed: results
            .total_bytes_processed
            .or(statistics.total_bytes_processed),
        cache_hit: results.cache_hit.or(statistics.cache_hit),
    })
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
