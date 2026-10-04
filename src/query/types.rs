use crate::{BigQueryDatasetRef, BigQueryFieldType, BigQueryReadOptions, BigQueryTableSchema};
use gcloud_sdk::google::cloud::bigquery::v2::QueryParameter;
use rsb_derive::Builder;
use std::collections::BTreeMap;
use std::time::Duration;

/// What a query sends: the statement, its parameters and the job settings, as
/// [`BigQueryQueryBuilder`](crate::BigQueryQueryBuilder) collects them.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct BigQueryQueryParams {
    /// The GoogleSQL statement.
    pub sql: String,
    /// The parameters, encoded. Named ones carry their name, positional ones an empty name.
    #[default = "Vec::new()"]
    pub(crate) query_parameters: Vec<QueryParameter>,
    /// Where the job runs. Unset, the client's
    /// [`location`](crate::BigQueryDbOptions::location) is sent, and with neither BigQuery
    /// finds the location from the tables the statement reads.
    pub location: Option<String>,
    /// The dataset that unqualified table names resolve in.
    pub default_dataset: Option<BigQueryDatasetRef>,
    /// Labels attached to the job.
    #[default = "BTreeMap::new()"]
    pub labels: BTreeMap<String, String>,
    /// The job fails without running if it would bill more bytes than this.
    pub maximum_bytes_billed: Option<i64>,
    /// Whether a cached result may be returned. BigQuery's default is `true`.
    pub use_query_cache: Option<bool>,
    /// How long the first `Query` call waits for the job before the client polls it. Defaults to
    /// 10 seconds, BigQuery's own default.
    pub timeout: Option<Duration>,
    /// How long BigQuery lets the job run before it cancels it.
    pub job_timeout: Option<Duration>,
    /// The idempotency key of the `Query` call. Unset, each terminal call sends a fresh random
    /// one, which every retry of that call repeats.
    pub request_id: Option<String>,
    /// The most rows the first response may carry inline. A result with more is read through
    /// the Storage Read API from the job's destination table.
    ///
    /// Unset by default, so BigQuery decides: it sent results of up to 364 KB of Arrow inline
    /// and paged results of 485 KB and more, and below that bound the inline result reached
    /// its last row about 2.7 times sooner than a Storage Read session.
    pub inline_rows_limit: Option<u32>,
    /// How a result read through the Storage Read API opens its session.
    #[default = "BigQueryReadOptions::new()"]
    pub read_options: BigQueryReadOptions,
}

/// The type of a query parameter, for a value whose type cannot be inferred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryParamType {
    /// The scalar type, or the element type of an array.
    pub field_type: BigQueryFieldType,
    /// Whether the parameter is an ARRAY of `field_type`.
    pub repeated: bool,
}

impl From<BigQueryFieldType> for BigQueryParamType {
    fn from(field_type: BigQueryFieldType) -> Self {
        Self {
            field_type,
            repeated: false,
        }
    }
}

impl BigQueryParamType {
    /// An ARRAY of `element`.
    pub fn array_of(element: BigQueryFieldType) -> Self {
        Self {
            field_type: element,
            repeated: true,
        }
    }
}

/// What a statement did, as the `Query` response reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryQueryOutcome {
    /// The job that ran the statement.
    pub job: Option<BigQueryJobRef>,
    /// `SELECT`, `INSERT`, `CREATE_TABLE`, and so on.
    pub statement_type: Option<String>,
    /// Rows a DML statement changed.
    pub num_dml_affected_rows: Option<i64>,
    /// Rows a DML statement inserted, updated and deleted.
    pub dml_stats: Option<BigQueryDmlStats>,
    /// Rows in the result.
    pub total_rows: Option<u64>,
    /// Bytes the statement processed.
    pub total_bytes_processed: Option<i64>,
    /// Whether the result came from the query cache.
    pub cache_hit: Option<bool>,
}

/// The rows a DML statement changed, by kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BigQueryDmlStats {
    /// Rows inserted.
    pub inserted: i64,
    /// Rows updated.
    pub updated: i64,
    /// Rows deleted.
    pub deleted: i64,
}

/// What a dry run reports without running the statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryDryRunResult {
    /// Bytes the statement would process.
    pub total_bytes_processed: Option<i64>,
    /// The schema of the result.
    pub schema: Option<BigQueryTableSchema>,
}

/// A BigQuery job.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct BigQueryJobRef {
    /// The project that runs the job.
    pub project_id: String,
    /// The job's ID.
    pub job_id: String,
    /// Where the job runs, when BigQuery reported it.
    pub location: Option<String>,
}

impl From<gcloud_sdk::google::cloud::bigquery::v2::JobReference> for BigQueryJobRef {
    fn from(job: gcloud_sdk::google::cloud::bigquery::v2::JobReference) -> Self {
        Self {
            project_id: job.project_id,
            job_id: job.job_id,
            location: job.location.filter(|l| !l.is_empty()),
        }
    }
}

impl From<gcloud_sdk::google::cloud::bigquery::v2::DmlStats> for BigQueryDmlStats {
    /// BigQuery leaves out the counts of the kinds a statement did not do, so a count missing
    /// from reported stats is zero.
    fn from(stats: gcloud_sdk::google::cloud::bigquery::v2::DmlStats) -> Self {
        Self {
            inserted: stats.inserted_row_count.unwrap_or(0),
            updated: stats.updated_row_count.unwrap_or(0),
            deleted: stats.deleted_row_count.unwrap_or(0),
        }
    }
}
